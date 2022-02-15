use super::*;
use pin_project::pin_project;
use std::future::Future;

pub trait ModFuture<M>: 'static
where
    M: Module,
{
    type Output: Send + 'static;

    fn poll(
        self: Pin<&mut Self>,
        task: &mut Context<'_>,
        module: &mut M,
        ctx: &mut RunCtx<M>,
    ) -> Poll<Self::Output>;
}

pub trait ModFutureExt<M>
where
    M: Module,
{
    fn then<FutN, F>(self, f: F) -> Then<M, Self, FutN, F>
    where
        Self: ModFuture<M> + Sized,
        FutN: ModFuture<M>,
        F: FnOnce(Self::Output, &mut M, &mut RunCtx<M>) -> FutN;
}

impl<T, M> ModFutureExt<M> for T
where
    T: ModFuture<M>,
    M: Module,
{
    fn then<FutN, F>(self, f: F) -> Then<M, Self, FutN, F>
    where
        Self: ModFuture<M>,
        FutN: ModFuture<M>,
        F: FnOnce(<Self as ModFuture<M>>::Output, &mut M, &mut RunCtx<M>) -> FutN,
    {
        Then::Prev {
            fut: self,
            action: Some(f),
            _p: Default::default(),
        }
    }
}

#[pin_project(project = ThenProj)]
pub enum Then<M, FutP, FutN, F>
where
    M: Module,
    FutP: ModFuture<M>,
    FutN: ModFuture<M>,
    F: FnOnce(FutP::Output, &mut M, &mut RunCtx<M>) -> FutN,
{
    Prev {
        #[pin]
        fut: FutP,
        action: Option<F>,
        _p: PhantomData<M>,
    },
    Next {
        #[pin]
        fut: FutN,
    },
    Empty,
}

impl<M, FutP, FutN, F> ModFuture<M> for Then<M, FutP, FutN, F>
where
    M: Module,
    FutP: ModFuture<M>,
    FutN: ModFuture<M>,
    F: FnOnce(FutP::Output, &mut M, &mut RunCtx<M>) -> FutN + 'static,
{
    type Output = FutN::Output;
    fn poll(
        mut self: Pin<&mut Self>,
        task: &mut Context<'_>,
        module: &mut M,
        ctx: &mut RunCtx<M>,
    ) -> Poll<Self::Output> {
        match self.as_mut().project() {
            ThenProj::Prev { fut, action, _p } => {
                let output = poll_ready!(fut.poll(task, module, ctx));
                let action = action.take().unwrap();
                let futn = action(output, module, ctx);
                self.set(Then::Next { fut: futn });
                self.poll(task, module, ctx)
            }
            ThenProj::Next { fut } => {
                let output = poll_ready!(fut.poll(task, module, ctx));
                self.set(Then::Empty);
                Poll::Ready(output)
            }
            ThenProj::Empty => panic!("Poll after finished"),
        }
    }
}

#[pin_project]
pub struct FutureWrap<F>
where
    F: Future,
{
    #[pin]
    f: F,
}

impl<M, F> ModFuture<M> for FutureWrap<F>
where
    M: Module,
    F: Future + 'static,
    F::Output: Send + 'static,
{
    type Output = F::Output;

    fn poll(
        self: Pin<&mut Self>,
        task: &mut Context<'_>,
        module: &mut M,
        ctx: &mut RunCtx<M>,
    ) -> Poll<Self::Output> {
        self.project().f.poll(task)
    }
}

pub fn wrap_future<Fut>(fut: Fut) -> FutureWrap<Fut>
where
    Fut: Future,
{
    FutureWrap { f: fut }
}
