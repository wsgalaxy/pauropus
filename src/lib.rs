use std::{
    any::Any,
    marker::PhantomData,
    pin::Pin,
    task::{Context, Poll},
};

pub enum CtlMsg {
    ForModule(String, Box<dyn Any>),
}

pub struct CtlRsp(String, Box<dyn Any>);

pub trait Module: Send + Sized + 'static {
    type WI: Send + 'static;
    type WO: Send + 'static;
    type RI: Send + 'static;
    type RO: Send + 'static;

    type HandleMsgWr: AsyncOpCtx<Self, Output = ()>;
    type HandleMsgRd: AsyncOpCtx<Self, Output = ()>;
    type HandleMsgCtl: AsyncOpCtx<Self, Output = Option<CtlRsp>>;

    fn handle_msg_wr(&mut self, ctx: &mut RunCtx<Self>, msg: Self::WI) -> Self::HandleMsgWr;
    fn handle_msg_rd(&mut self, ctx: &mut RunCtx<Self>, msg: Self::RI) -> Self::HandleMsgRd;
    fn handle_msg_ctl(&mut self, ctx: &mut RunCtx<Self>, msg: &CtlMsg) -> Self::HandleMsgCtl;
    fn get_name(&self) -> &str;
    fn started(&mut self);
    fn stopped(&mut self);
}

pub trait AsyncOpCtx<M>
where
    M: Module,
{
    type Output: Send + 'static;

    fn poll(
        &mut self,
        task: Context<'_>,
        module: &mut M,
        ctx: &mut RunCtx<M>,
    ) -> Poll<Self::Output>;
}

pub struct RunCtx<M>
where
    M: Module,
{
    _p: PhantomData<M>,
}

impl<M> RunCtx<M>
where
    M: Module,
{
    pub fn to_wq_next(&mut self, msg: M::WO) -> ToMsgQueue<(), M::WO> {
        ToMsgQueue {
            _p: Default::default(),
        }
    }

    pub fn to_wq_self(&mut self, msg: M::WI) -> ToMsgQueue<(), M::WI> {
        ToMsgQueue {
            _p: Default::default(),
        }
    }

    pub fn to_rd_next(&mut self, msg: M::RO) -> ToMsgQueue<(), M::RO> {
        ToMsgQueue {
            _p: Default::default(),
        }
    }

    pub fn to_rd_self(&mut self, msg: M::RI) -> ToMsgQueue<(), M::RI> {
        ToMsgQueue {
            _p: Default::default(),
        }
    }
}

pub struct ToMsgQueue<Q, Msg> {
    _p: PhantomData<(Q, Msg)>,
}

impl<M, Q, Msg> AsyncOpCtx<M> for ToMsgQueue<Q, Msg>
where
    M: Module,
{
    type Output = ();

    fn poll(
        &mut self,
        task: Context<'_>,
        module: &mut M,
        ctx: &mut RunCtx<M>,
    ) -> Poll<Self::Output> {
        // TODO: Send data to queue
        Poll::Pending
    }
}

pub struct Handle<M>
where
    M: Module,
{
    _p1: PhantomData<M>,
}

impl<M> Handle<M>
where
    M: Module,
{
    /// Transfer a module into handle.
    pub fn from_module(m: M) -> Self {
        // TODO
        Self {
            _p1: Default::default(),
        }
    }
}
