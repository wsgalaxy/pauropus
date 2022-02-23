use std::{
    any::Any,
    future::Future,
    marker::PhantomData,
    pin::Pin,
    task::{Context, Poll},
};
use tokio::sync::{
    mpsc::error::{SendError, TrySendError},
    oneshot,
};

pub mod chan;
pub mod fut;
pub mod util;

use self::fut::ModFuture;
use self::util::ReusableBoxFuture;

pub enum CtlMsg {
    Start,
    Stop,
    ForModule(String, Box<dyn Any + Send + 'static>),
}

pub struct CtlRsp(String, Box<dyn Any + Send + 'static>);

pub trait Module: Send + Sized + Unpin + 'static {
    type WI: Send + 'static;
    type WO: Send + 'static;
    type RI: Send + 'static;
    type RO: Send + 'static;

    fn handle_msg_wr(&mut self, ctx: &mut RunCtx<Self>, msg: Self::WI) -> WrMsgCompletion<Self>;
    fn handle_msg_rd(&mut self, ctx: &mut RunCtx<Self>, msg: Self::RI) -> RdMsgCompletion<Self>;
    fn handle_msg_ctl(&mut self, ctx: &mut RunCtx<Self>, msg: &CtlMsg) -> CtlMsgCompletion<Self>;
    fn get_name(&self) -> &str;
    fn started(&mut self);
    fn stopped(&mut self);
}

type CtlEnvelope = (CtlMsg, Vec<CtlRsp>, oneshot::Sender<Vec<CtlRsp>>);

pub struct RunCtx<M>
where
    M: Module,
{
    wi_rx: chan::Receiver<M::WI>,
    wo_tx: Option<chan::Sender<M::WO>>,
    ri_rx: chan::Receiver<M::RI>,
    ro_tx: Option<chan::Sender<M::RO>>,
    ctl_rx: chan::Receiver<CtlEnvelope>,
    ctl_tx: Option<chan::Sender<CtlEnvelope>>,

    wr_comp: Option<ReusableBoxFuture<(), M>>,
    wr_comp_polling: bool,
    rd_comp: Option<ReusableBoxFuture<(), M>>,
    rd_comp_polling: bool,
    ctl_comp: Option<ReusableBoxFuture<Option<CtlRsp>, M>>,
    ctl_comp_polling: bool,

    ctl_env: Option<CtlEnvelope>,
}

impl<M> RunCtx<M>
where
    M: Module,
{
    async fn run(self, m: M) {
        RunCtxFuture {
            run_ctx: Some(Box::new(self)),
            module: Some(Box::new(m)),
        }
        .await
    }

    pub fn complete_wr(&mut self) -> WrMsgCompletion<M> {
        assert!(self.wr_comp_polling == false);

        MsgCompletion::Instant((), Default::default())
    }

    pub fn complete_wr_later<Fut>(&mut self, fut: Fut) -> WrMsgCompletion<M>
    where
        Fut: ModFuture<M, Output = ()>,
    {
        assert!(self.wr_comp_polling == false);

        self.wr_comp_polling = true;
        if self.wr_comp.is_some() {
            let mut _box = self.wr_comp.take().unwrap();
            _box.set(fut);
            MsgCompletion::Later(_box)
        } else {
            MsgCompletion::Later(ReusableBoxFuture::new(fut))
        }
    }

    pub fn complete_rd(&mut self) -> RdMsgCompletion<M> {
        assert!(self.rd_comp_polling == false);

        MsgCompletion::Instant((), Default::default())
    }

    pub fn complete_rd_later<Fut>(&mut self, fut: Fut) -> RdMsgCompletion<M>
    where
        Fut: ModFuture<M, Output = ()>,
    {
        assert!(self.rd_comp_polling == false);

        self.rd_comp_polling = true;
        if self.rd_comp.is_some() {
            let mut _box = self.rd_comp.take().unwrap();
            _box.set(fut);
            MsgCompletion::Later(_box)
        } else {
            MsgCompletion::Later(ReusableBoxFuture::new(fut))
        }
    }

    pub fn complete_ctl(&mut self, v: Option<CtlRsp>) -> CtlMsgCompletion<M> {
        assert!(self.ctl_comp_polling == false);

        MsgCompletion::Instant(v, Default::default())
    }

    pub fn complete_ctl_later<Fut>(&mut self, fut: Fut) -> CtlMsgCompletion<M>
    where
        Fut: ModFuture<M, Output = Option<CtlRsp>>,
    {
        assert!(self.ctl_comp_polling == false);

        self.ctl_comp_polling = true;
        if self.ctl_comp.is_some() {
            let mut _box = self.ctl_comp.take().unwrap();
            _box.set(fut);
            MsgCompletion::Later(_box)
        } else {
            MsgCompletion::Later(ReusableBoxFuture::new(fut))
        }
    }

    pub fn to_wq_next(&mut self, msg: M::WO) -> impl Future {
        let tx = self.wo_tx.clone();
        async move {
            match tx {
                Some(tx) => tx.send(msg).await,
                None => Ok(()),
            }
        }
    }

    pub fn try_to_wq_next(&mut self, msg: M::WO) -> Result<(), TrySendError<M::WO>> {
        match &self.wo_tx {
            Some(tx) => tx.try_send(msg),
            None => Ok(()),
        }
    }

    pub fn to_wq_self(&mut self, msg: M::WI) -> Result<(), SendError<M::WI>> {
        let tx = self.wi_rx.sender();
        match tx {
            Some(tx) => tx.do_send(msg),
            None => Ok(()),
        }
    }

    pub fn to_rq_next(&mut self, msg: M::RO) -> impl Future {
        let tx = self.ro_tx.clone();
        async move {
            match tx {
                Some(tx) => tx.send(msg).await,
                None => Ok(()),
            }
        }
    }

    pub fn try_to_rq_next(&mut self, msg: M::RO) -> Result<(), TrySendError<M::RO>> {
        match &self.ro_tx {
            Some(tx) => tx.try_send(msg),
            None => Ok(()),
        }
    }

    pub fn to_rq_self(&mut self, msg: M::RI) -> Result<(), SendError<M::RI>> {
        let tx = self.ri_rx.sender();
        match tx {
            Some(tx) => tx.do_send(msg),
            None => Ok(()),
        }
    }
}

enum PollResult {
    Pending,
    Again,
}

struct RunCtxFuture<M>
where
    M: Module,
{
    run_ctx: Option<Box<RunCtx<M>>>,
    module: Option<Box<M>>,
}

// Poll wi_rx/ri_rx.
macro_rules! poll_channel {
    ($fn_name:ident, $chan:ident, $comp:ident, $comp_polling:ident, $handler:ident, $handle_value:ident, $handle_res:ident) => {
        fn $fn_name(cx: &mut Context<'_>, run_ctx: &mut RunCtx<M>, module: &mut M) -> PollResult {
            for _ in 0..128 {
                if run_ctx.$comp_polling {
                    assert!(run_ctx.$comp.is_some());
                    let mut $comp = run_ctx.$comp.take().unwrap();
                    let res = $comp.poll(cx, module, run_ctx);
                    // Store the $comp back so that we can reuse it.
                    run_ctx.$comp = Some($comp);
                    match res {
                        Poll::Pending => {
                            // We can not handle new msg from channel.
                            return PollResult::Pending;
                        }
                        Poll::Ready(_) => {
                            run_ctx.$comp_polling = false;
                        }
                    }
                }
                // Handle new msg
                match run_ctx.$chan.poll_recv(cx) {
                    Poll::Pending => {
                        // No msg from channel.
                        return PollResult::Pending;
                    }
                    Poll::Ready(None) => {
                        // The channel is dead.
                        // TODO: Close it.
                        return PollResult::Pending;
                    }
                    Poll::Ready(Some(msg)) => {
                        match $handle_value(run_ctx, module, msg) {
                            MsgCompletion::Instant(_, _) => {
                                // Ready to handle next msg.
                                continue;
                            }
                            MsgCompletion::Later(mut _box) => {
                                match _box.poll(cx, module, run_ctx) {
                                    Poll::Pending => {
                                        // This msg need to be polled again.
                                        run_ctx.$comp = Some(_box);
                                        run_ctx.$comp_polling = true;
                                        return PollResult::Pending;
                                    }
                                    Poll::Ready(v) => {
                                        $handle_res(run_ctx, module, v);
                                        // Recycle box for next use.
                                        run_ctx.$comp = Some(_box);
                                        // Ready to handle next msg.
                                        continue;
                                    }
                                }
                            }
                        }
                    }
                }
            }
            // Need to be pooled again.
            PollResult::Again
        }
    };
}

fn handle_value_ri<M>(run_ctx: &mut RunCtx<M>, module: &mut M, msg: M::RI) -> RdMsgCompletion<M>
where
    M: Module,
{
    module.handle_msg_rd(run_ctx, msg)
}

fn handle_value_wi<M>(run_ctx: &mut RunCtx<M>, module: &mut M, msg: M::WI) -> WrMsgCompletion<M>
where
    M: Module,
{
    module.handle_msg_wr(run_ctx, msg)
}

fn handle_value_ctl<M>(
    run_ctx: &mut RunCtx<M>,
    module: &mut M,
    msg: CtlEnvelope,
) -> CtlMsgCompletion<M>
where
    M: Module,
{
    let res = module.handle_msg_ctl(run_ctx, &msg.0);
    run_ctx.ctl_env = Some(msg);
    res
}

fn handle_res_wr<M, T>(run_ctx: &mut RunCtx<M>, module: &mut M, _v: T)
where
    M: Module,
    T: Send + 'static,
{
}

fn handle_res_ctl<M>(run_ctx: &mut RunCtx<M>, module: &mut M, v: Option<CtlRsp>)
where
    M: Module,
{
    assert!(run_ctx.ctl_env.is_some());
    let mut env = run_ctx.ctl_env.take().unwrap();
    if let Some(rsp) = v {
        env.1.push(rsp);
    }
    // Send CtlEnvelop to next module
    if let Some(ref mut tx) = run_ctx.ctl_tx {
        match tx.do_send(env) {
            Ok(_) => {}
            Err(SendError(env)) => {
                // Failed to send to next module, just response.
                let (_, rsps, rsp_tx) = env;
                let _ = rsp_tx.send(rsps);
            }
        }
    } else {
        // Just response.
        let (_, rsps, rsp_tx) = env;
        let _ = rsp_tx.send(rsps);
    }
}

impl<M> RunCtxFuture<M>
where
    M: Module,
{
    poll_channel!(
        poll_wi,
        wi_rx,
        wr_comp,
        wr_comp_polling,
        handle_msg_wr,
        handle_value_wi,
        handle_res_wr
    );
    poll_channel!(
        poll_ri,
        ri_rx,
        rd_comp,
        rd_comp_polling,
        handle_msg_rd,
        handle_value_ri,
        handle_res_wr
    );
    poll_channel!(
        poll_ctl,
        ctl_rx,
        ctl_comp,
        ctl_comp_polling,
        handle_msg_ctl,
        handle_value_ctl,
        handle_res_ctl
    );
}

impl<M> Future for RunCtxFuture<M>
where
    M: Module,
{
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut run_ctx = self.run_ctx.take().unwrap();
        let mut module = self.module.take().unwrap();

        let mut wi_poll_again = true;
        let mut ri_poll_again = true;
        let mut ctl_poll_again = true;
        while wi_poll_again || ri_poll_again || ctl_poll_again {
            // Poll wi_rx.
            if wi_poll_again {
                match RunCtxFuture::poll_wi(cx, &mut run_ctx, &mut module) {
                    PollResult::Pending => wi_poll_again = false,
                    PollResult::Again => {}
                }
            }

            // Poll ri_rx.
            if ri_poll_again {
                match RunCtxFuture::poll_ri(cx, &mut run_ctx, &mut module) {
                    PollResult::Pending => ri_poll_again = false,
                    PollResult::Again => {}
                }
            }

            // Poll ctl_rx.
            if ctl_poll_again {
                match RunCtxFuture::poll_ctl(cx, &mut run_ctx, &mut module) {
                    PollResult::Pending => ctl_poll_again = false,
                    PollResult::Again => {}
                }
            }
        }
        self.run_ctx = Some(run_ctx);
        self.module = Some(module);
        // TODO: Handle Pipeline exit.
        Poll::Pending
    }
}

pub trait MsgType: Send + 'static {}
pub struct MsgTypeWr {}
impl MsgType for MsgTypeWr {}
pub struct MsgTypeRd {}
impl MsgType for MsgTypeRd {}
pub struct MsgTypeCtl {}
impl MsgType for MsgTypeCtl {}

pub enum MsgCompletion<M, T, MT>
where
    M: Module,
    T: Send + 'static,
    MT: MsgType,
{
    Instant(T, PhantomData<MT>),
    Later(ReusableBoxFuture<T, M>),
}

type WrMsgCompletion<M> = MsgCompletion<M, (), MsgTypeWr>;
type RdMsgCompletion<M> = MsgCompletion<M, (), MsgTypeRd>;
type CtlMsgCompletion<M> = MsgCompletion<M, Option<CtlRsp>, MsgTypeCtl>;

pub struct Handle<WI, RO>
where
    WI: Send + 'static,
    RO: Send + 'static,
{
    wi_tx: chan::Sender<WI>,
    ro_tx_setter: oneshot::Sender<chan::Sender<RO>>,
    ctl_tx: chan::Sender<CtlEnvelope>,
}

impl<WI, RO> Handle<WI, RO>
where
    WI: Send + 'static,
    RO: Send + 'static,
{
    pub fn stack<M>(self, m: M, buffer_size: usize) -> Handle<M::WI, M::RO>
    where
        M: Module<WO = WI, RI = RO>,
    {
        let (wi_tx, wi_rx) = chan::channel(buffer_size);
        let (ri_tx, ri_rx) = chan::channel(buffer_size);
        let (ctl_tx, ctl_rx) = chan::channel(buffer_size);

        if let Err(_) = self.ro_tx_setter.send(ri_tx) {
            panic!("Failed to setup ri_tx");
        }

        let mut ctx = RunCtx::<M> {
            wi_rx,
            wo_tx: Some(self.wi_tx),
            ri_rx,
            ro_tx: None,
            ctl_rx,
            ctl_tx: Some(self.ctl_tx),

            wr_comp_polling: false,
            wr_comp: None,
            rd_comp_polling: false,
            rd_comp: None,
            ctl_comp_polling: false,
            ctl_comp: None,

            ctl_env: None,
        };

        let (ro_tx_setter, ro_tx_receiver) = oneshot::channel();
        tokio::spawn(async move {
            // Run ctx here.
            let ro_tx = match ro_tx_receiver.await {
                Err(_) => return,
                Ok(v) => v,
            };
            ctx.ro_tx = Some(ro_tx);
            ctx.run(m).await;
        });

        Handle {
            wi_tx,
            ro_tx_setter,
            ctl_tx,
        }
    }

    pub fn finish(self) -> Pipeline<WI, RO> {
        let (ro_tx, ro_rx) = chan::channel(1024);
        let _ = self.ro_tx_setter.send(ro_tx);

        Pipeline {
            wi_tx: self.wi_tx,
            ro_rx,
            ctl_tx: self.ctl_tx,
        }
    }
}

pub struct Pipeline<WI, RO>
where
    WI: Send + 'static,
    RO: Send + 'static,
{
    wi_tx: chan::Sender<WI>,
    ro_rx: chan::Receiver<RO>,
    ctl_tx: chan::Sender<CtlEnvelope>,
}

impl<WI, RO> Pipeline<WI, RO>
where
    WI: Send + 'static,
    RO: Send + 'static,
{
    pub fn begin() -> Handle<WI, RO> {
        let (wi_tx, wi_rx) = chan::channel(1024);
        let (ctl_tx, ctl_rx) = chan::channel(1024);
        let (ro_tx_setter, ro_tx_receiver) = oneshot::channel();

        let mut tail = PipelineTail::<WI, RO> {
            wi_rx,
            ro_tx: None,
            ctl_rx,
        };
        tokio::spawn(async move {
            let ro_tx = match ro_tx_receiver.await {
                Err(_) => return,
                Ok(v) => v,
            };
            tail.ro_tx = Some(ro_tx);
            tail.run().await;
        });

        Handle {
            wi_tx,
            ro_tx_setter,
            ctl_tx,
        }
    }

    pub fn poll_read(&mut self, cx: &mut Context<'_>) -> Poll<Option<RO>> {
        self.ro_rx.poll_recv(cx)
    }

    pub async fn read(&mut self) -> Option<RO> {
        self.ro_rx.recv().await
    }

    pub async fn write(&mut self, msg: WI) -> Result<(), SendError<WI>> {
        self.wi_tx.send(msg).await
    }

    pub fn try_write(&mut self, msg: WI) -> Result<(), TrySendError<WI>> {
        self.wi_tx.try_send(msg)
    }

    pub async fn send_ctl(&mut self, ctl_msg: CtlMsg) -> Result<Vec<CtlRsp>, SendError<CtlMsg>> {
        let (rsp_tx, rsp_rx) = oneshot::channel();
        if let Err(e) = self.ctl_tx.send((ctl_msg, Vec::new(), rsp_tx)).await {
            return Err(SendError(e.0 .0));
        }
        match rsp_rx.await {
            // If no resp, return a empty vector.
            Err(_) => Ok(Vec::new()),
            Ok(v) => Ok(v),
        }
    }

    pub fn splict(
        self,
    ) -> (
        PipelineWritePart<WI>,
        PipelineReadPart<RO>,
        PipelineCtlpPart,
    ) {
        (
            PipelineWritePart { wi_tx: self.wi_tx },
            PipelineReadPart { ro_rx: self.ro_rx },
            PipelineCtlpPart {
                ctl_tx: self.ctl_tx,
            },
        )
    }
}

pub struct PipelineWritePart<WI>
where
    WI: Send + 'static,
{
    wi_tx: chan::Sender<WI>,
}

impl<WI> PipelineWritePart<WI>
where
    WI: Send + 'static,
{
    pub async fn write(&mut self, msg: WI) -> Result<(), SendError<WI>> {
        self.wi_tx.send(msg).await
    }

    pub fn try_write(&mut self, msg: WI) -> Result<(), TrySendError<WI>> {
        self.wi_tx.try_send(msg)
    }
}

pub struct PipelineReadPart<RO>
where
    RO: Send + 'static,
{
    ro_rx: chan::Receiver<RO>,
}

impl<RO> PipelineReadPart<RO>
where
    RO: Send + 'static,
{
    pub fn poll_read(&mut self, cx: &mut Context<'_>) -> Poll<Option<RO>> {
        self.ro_rx.poll_recv(cx)
    }

    pub async fn read(&mut self) -> Option<RO> {
        self.ro_rx.recv().await
    }
}

pub struct PipelineCtlpPart {
    ctl_tx: chan::Sender<CtlEnvelope>,
}

impl PipelineCtlpPart {
    pub async fn send_ctl(&mut self, ctl_msg: CtlMsg) -> Result<Vec<CtlRsp>, SendError<CtlMsg>> {
        let (rsp_tx, rsp_rx) = oneshot::channel();
        if let Err(e) = self.ctl_tx.send((ctl_msg, Vec::new(), rsp_tx)).await {
            return Err(SendError(e.0 .0));
        }
        match rsp_rx.await {
            // If no resp, return a empty vector.
            Err(_) => Ok(Vec::new()),
            Ok(v) => Ok(v),
        }
    }
}

struct PipelineTail<WI, RO>
where
    WI: Send + 'static,
    RO: Send + 'static,
{
    wi_rx: chan::Receiver<WI>,
    ro_tx: Option<chan::Sender<RO>>,
    ctl_rx: chan::Receiver<CtlEnvelope>,
}

impl<WI, RO> PipelineTail<WI, RO>
where
    WI: Send + 'static,
    RO: Send + 'static,
{
    fn run(self) -> impl Future {
        PipelineTailRun { tail: self }
    }
}

struct PipelineTailRun<WI, RO>
where
    WI: Send + 'static,
    RO: Send + 'static,
{
    tail: PipelineTail<WI, RO>,
}

impl<WI, RO> Future for PipelineTailRun<WI, RO>
where
    WI: Send + 'static,
    RO: Send + 'static,
{
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut ctl_pending = false;
        let mut wi_pending = false;

        while !ctl_pending || !wi_pending {
            for _ in 0..128 {
                match self.tail.ctl_rx.poll_recv(cx) {
                    Poll::Pending => {
                        ctl_pending = true;
                        break;
                    }
                    Poll::Ready(None) => {
                        // TODO: ctl_rx is dead, close it.
                        ctl_pending = true;
                        break;
                    }
                    Poll::Ready(Some(v)) => {
                        // Send all rsp to the receiver.
                        let (msg, rsp, tx) = v;

                        match msg {
                            CtlMsg::Stop => {
                                // Stop polling.
                                return Poll::Ready(());
                            }
                            // Any other msg will be ignored.
                            _ => {}
                        }

                        let _ = tx.send(rsp);
                    }
                }
            }

            for _ in 0..128 {
                match self.tail.wi_rx.poll_recv(cx) {
                    Poll::Pending => {
                        wi_pending = true;
                        break;
                    }
                    Poll::Ready(None) => {
                        // TODO: wi_rx is dead, close it.
                        wi_pending = true;
                        break;
                    }
                    // Do nothing but drop the msg.
                    Poll::Ready(_) => {}
                }
            }
        }

        Poll::Pending
    }
}

#[macro_export]
macro_rules! poll_ready {
    ($x:expr) => {
        match $x {
            Poll::Pending => {
                return Poll::Pending;
            }
            Poll::Ready(v) => v,
        }
    };
}
