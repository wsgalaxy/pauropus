use pin_project::{pin_project, pinned_drop};
use std::{
    alloc::{dealloc, Layout},
    any::{Any, TypeId},
    marker::PhantomData,
    mem::forget,
    ops::Deref,
    pin::Pin,
    task::{Context, Poll},
};

pub mod chan;
pub mod fut;

use self::fut::ModFuture;

pub enum CtlMsg {
    ForModule(String, Box<dyn Any + Send + 'static>),
}

pub struct CtlRsp(String, Box<dyn Any + Send + 'static>);

pub trait Module: Send + Sized + 'static {
    type WI: Send + 'static;
    type WO: Send + 'static;
    type RI: Send + 'static;
    type RO: Send + 'static;

    type HandleMsgWr: ModFuture<Self, Output = ()>;
    type HandleMsgRd: ModFuture<Self, Output = ()>;
    type HandleMsgCtl: ModFuture<Self, Output = Option<CtlRsp>>;

    fn handle_msg_wr(
        &mut self,
        ctx: &mut RunCtx<Self>,
        msg: Self::WI,
    ) -> HandleCompletion<Self, ()>;
    fn handle_msg_rd(
        &mut self,
        ctx: &mut RunCtx<Self>,
        msg: Self::RI,
    ) -> HandleCompletion<Self, ()>;
    fn handle_msg_ctl(
        &mut self,
        ctx: &mut RunCtx<Self>,
        msg: &CtlMsg,
    ) -> HandleCompletion<Self, Option<CtlRsp>>;
    fn get_name(&self) -> &str;
    fn started(&mut self);
    fn stopped(&mut self);
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
    pub fn complete<T>(&mut self, v: T) -> HandleCompletion<M, T>
    where
        T: Send + 'static,
    {
        HandleCompletion::Instant(v)
    }

    pub fn complete_later<Fut, T>(&mut self, fut: Fut) -> HandleCompletion<M, T>
    where
        Fut: ModFuture<M, Output = T>,
        T: Send + 'static,
    {
        // TODO: Reuse cached ToBeSolved if possible
        let mut later = ToBeSolved::new();
        later.as_mut().store(fut);
        HandleCompletion::Later(later)
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

pub enum HandleCompletion<M, T>
where
    M: Module,
    T: Send + 'static,
{
    Instant(T),
    Later(Pin<Box<ToBeSolved<M, T>>>),
}

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
        // Future resolved, it is time to drop future and cache the memory for next use
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
            // Replacing the previous future
            assert!(self.cache.is_none());
            assert!(self.typeid.is_some());
            assert!(self.layout.is_some());
            if self.typeid.unwrap().ne(&(f.type_id())) {
                // Type mismatch, We need reallocate memory
                self.typeid = Some(f.type_id());
                self.layout = Some(Layout::for_value(&f));
                self.fut = Some(Box::new(f));
            } else {
                // Type match, Just drop previou future and move f to it
                let addr =
                    self.fut.as_ref().unwrap().deref() as *const (dyn ModFuture<M, Output = O>);
                let addr = addr as *const Fut as *mut Fut;
                std::mem::swap(&mut f, &mut *addr);
                // Drop the previou future
                drop(f);
            }
        } else if self.cache.is_some() {
            // We have cached buffer, no need to allocate new memory any more
            assert!(self.typeid.is_some());
            assert!(self.layout.is_some());
            if self.typeid.unwrap().ne(&(f.type_id())) {
                // Type mismatch, reallocate anyway
                let addr = Box::into_raw(self.cache.take().unwrap());
                dealloc(addr as *mut u8, self.layout.take().unwrap());
                self.typeid = Some(f.type_id());
                self.layout = Some(Layout::for_value(&f));
                self.fut = Some(Box::new(f));
            } else {
                // Type match, reuse the previous memory
                self.fut = Some(self.cache.take().unwrap());
                let dst = self.fut.as_ref().unwrap().deref()
                    as *const (dyn ModFuture<M, Output = O>) as *const u8
                    as *mut u8;
                let src = &f as *const Fut as *const u8;
                std::ptr::copy(src, dst, self.layout.as_ref().unwrap().size());
                // No need to call destructor for f now as it contain a moved value
                forget(f);
            }
        } else {
            self.typeid = Some(f.type_id());
            self.layout = Some(Layout::for_value(&f));
            self.fut = Some(Box::new(f));
        }
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
