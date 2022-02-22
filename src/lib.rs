use pin_project::{pin_project, pinned_drop};
use std::{
    alloc::{dealloc, Layout},
    any::{Any, TypeId},
    future::Future,
    marker::PhantomData,
    mem::forget,
    ops::Deref,
    pin::Pin,
    task::{Context, Poll},
};
use tokio::sync::{mpsc, oneshot};

pub mod chan;
pub mod fut;

use self::fut::ModFuture;

pub enum CtlMsg {
    Start,
    Stop,
    ForModule(String, Box<dyn Any + Send + 'static>),
}

pub struct CtlRsp(String, Box<dyn Any + Send + 'static>);

pub trait Module: Send + Sized + 'static {
    type WI: Send + 'static;
    type WO: Send + 'static;
    type RI: Send + 'static;
    type RO: Send + 'static;

    fn handle_msg_wr(&mut self, ctx: &mut RunCtx<Self>, msg: Self::WI) -> MsgCompletion<Self, ()>;
    fn handle_msg_rd(&mut self, ctx: &mut RunCtx<Self>, msg: Self::RI) -> MsgCompletion<Self, ()>;
    fn handle_msg_ctl(
        &mut self,
        ctx: &mut RunCtx<Self>,
        msg: &CtlMsg,
    ) -> MsgCompletion<Self, Option<CtlRsp>>;
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
}

impl<M> RunCtx<M>
where
    M: Module,
{
    async fn run(self, m: M) {
        // TODO
    }

    pub fn complete_msg<T>(&mut self, v: T) -> MsgCompletion<M, T>
    where
        T: Send + 'static,
    {
        MsgCompletion::Instant(v)
    }

    pub fn complete_msg_later<Fut, T>(&mut self, fut: Fut) -> MsgCompletion<M, T>
    where
        Fut: ModFuture<M, Output = T>,
        T: Send + 'static,
    {
        // TODO: Reuse cached ToBeSolved if possible.
        let mut later = ToBeSolved::new();
        later.as_mut().store(fut);
        MsgCompletion::Later(later)
    }

    pub fn to_wq_next(&mut self, msg: M::WO) {
        // TODO
    }

    pub fn try_to_wq_next(&mut self, msg: M::WO) {
        // TODO
    }

    pub fn to_wq_self(&mut self, msg: M::WI) {
        // TODO
    }

    pub fn to_rd_next(&mut self, msg: M::RO) {
        // TODO
    }

    pub fn try_to_rd_next(&mut self, msg: M::RO) {
        // TODO
    }

    pub fn to_rd_self(&mut self, msg: M::RI) {
        // TODO
    }
}

pub enum MsgCompletion<M, T>
where
    M: Module,
    T: Send + 'static,
{
    Instant(T),
    Later(Pin<Box<ToBeSolved<M, T>>>),
}

// TODO: Use tokio_util::sync::ReusableBoxFuture instead.
#[pin_project(PinnedDrop)]
pub struct ToBeSolved<M, O>
where
    M: Module,
    O: Send + 'static,
{
    #[pin]
    fut: Option<Box<dyn ModFuture<M, Output = O>>>,
    cache: Option<Box<dyn ModFuture<M, Output = O>>>,
    typeid: Option<TypeId>,
    layout: Option<Layout>,
}

impl<M, O> ToBeSolved<M, O>
where
    M: Module,
    O: Send + 'static,
{
    pub fn new() -> Pin<Box<Self>> {
        Box::pin(Self {
            fut: None,
            cache: None,
            typeid: None,
            layout: None,
        })
    }

    pub fn poll(
        self: Pin<&mut Self>,
        task: &mut Context<'_>,
        module: &mut M,
        ctx: &mut RunCtx<M>,
    ) -> Poll<O> {
        assert!(self.fut.is_some(), "Polling an empty Future");
        assert!(self.cache.is_none());
        let mut prj = self.project();
        let output = poll_ready!(unsafe {
            prj.fut
                .as_mut()
                .map_unchecked_mut(|v| v.as_mut().unwrap().as_mut())
                .poll(task, module, ctx)
        });
        // Future resolved, it is time to drop future and cache the memory for next use.
        *(prj.cache) = Some(prj.fut.take().unwrap());
        unsafe {
            let addr = prj.cache.as_ref().unwrap().deref() as *const (dyn ModFuture<M, Output = O>)
                as *mut (dyn ModFuture<M, Output = O>);
            std::ptr::drop_in_place(addr);
        }
        Poll::Ready(output)
    }

    pub fn store<Fut>(self: Pin<&mut Self>, f: Fut)
    where
        Fut: ModFuture<M, Output = O>,
    {
        unsafe {
            self._store(f);
        }
    }

    unsafe fn _store<Fut>(mut self: Pin<&mut Self>, mut f: Fut)
    where
        Fut: ModFuture<M, Output = O>,
    {
        if self.fut.is_some() {
            // Replacing the previous future.
            assert!(self.cache.is_none());
            assert!(self.typeid.is_some());
            assert!(self.layout.is_some());
            if self.typeid.unwrap().ne(&(f.type_id())) {
                // Type mismatch, We need reallocate memory.
                self.typeid = Some(f.type_id());
                self.layout = Some(Layout::for_value(&f));
                self.fut = Some(Box::new(f));
            } else {
                // Type match, Just drop previou future and move f to it.
                let addr =
                    self.fut.as_ref().unwrap().deref() as *const (dyn ModFuture<M, Output = O>);
                let addr = addr as *const Fut as *mut Fut;
                std::mem::swap(&mut f, &mut *addr);
                // Drop the previou future.
                drop(f);
            }
        } else if self.cache.is_some() {
            // We have cached buffer, no need to allocate new memory any more.
            assert!(self.typeid.is_some());
            assert!(self.layout.is_some());
            if self.typeid.unwrap().ne(&(f.type_id())) {
                // Type mismatch, reallocate anyway.
                let addr = Box::into_raw(self.cache.take().unwrap());
                dealloc(addr as *mut u8, self.layout.take().unwrap());
                self.typeid = Some(f.type_id());
                self.layout = Some(Layout::for_value(&f));
                self.fut = Some(Box::new(f));
            } else {
                // Type match, reuse the previous memory.
                self.fut = Some(self.cache.take().unwrap());
                let dst = self.fut.as_ref().unwrap().deref()
                    as *const (dyn ModFuture<M, Output = O>) as *const u8
                    as *mut u8;
                let src = &f as *const Fut as *const u8;
                std::ptr::copy(src, dst, self.layout.as_ref().unwrap().size());
                // No need to call destructor for f now as it contain a moved value.
                forget(f);
            }
        } else {
            self.typeid = Some(f.type_id());
            self.layout = Some(Layout::for_value(&f));
            self.fut = Some(Box::new(f));
        }
    }

    pub fn is_resolved(&self) -> bool {
        self.fut.is_none()
    }
}

#[pinned_drop]
impl<M, O> PinnedDrop for ToBeSolved<M, O>
where
    M: Module,
    O: Send + 'static,
{
    fn drop(mut self: Pin<&mut Self>) {
        if let Some(b) = self.cache.take() {
            // Value contained in self.cache is invalid, do not call destructor for it.
            unsafe {
                dealloc(
                    Box::into_raw(b) as *const u8 as *mut u8,
                    self.layout.take().unwrap(),
                );
            }
        }
    }
}

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

    pub async fn write(&mut self, msg: WI) -> Result<(), mpsc::error::SendError<WI>> {
        self.wi_tx.send(msg).await
    }

    pub fn try_write(&mut self, msg: WI) -> Result<(), mpsc::error::TrySendError<WI>> {
        self.wi_tx.try_send(msg)
    }

    pub async fn send_ctl(
        &mut self,
        ctl_msg: CtlMsg,
    ) -> Result<Vec<CtlRsp>, mpsc::error::SendError<CtlMsg>> {
        let (rsp_tx, rsp_rx) = oneshot::channel();
        if let Err(e) = self.ctl_tx.send((ctl_msg, Vec::new(), rsp_tx)).await {
            return Err(mpsc::error::SendError(e.0 .0));
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
    pub async fn write(&mut self, msg: WI) -> Result<(), mpsc::error::SendError<WI>> {
        self.wi_tx.send(msg).await
    }

    pub fn try_write(&mut self, msg: WI) -> Result<(), mpsc::error::TrySendError<WI>> {
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
    pub async fn send_ctl(
        &mut self,
        ctl_msg: CtlMsg,
    ) -> Result<Vec<CtlRsp>, mpsc::error::SendError<CtlMsg>> {
        let (rsp_tx, rsp_rx) = oneshot::channel();
        if let Err(e) = self.ctl_tx.send((ctl_msg, Vec::new(), rsp_tx)).await {
            return Err(mpsc::error::SendError(e.0 .0));
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
