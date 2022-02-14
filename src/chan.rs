use parking_lot::Mutex;
use std::sync::Arc;
use tokio::sync::{mpsc, OwnedSemaphorePermit, Semaphore};

struct Chan<T>
where
    T: Send + 'static,
{
    inner: Mutex<Inner<T>>,
    sem: Arc<Semaphore>,
}

impl<T> Chan<T>
where
    T: Send + 'static,
{
    fn new(
        buffer: usize,
    ) -> (
        Self,
        mpsc::UnboundedSender<(Option<OwnedSemaphorePermit>, T)>,
        mpsc::UnboundedReceiver<(Option<OwnedSemaphorePermit>, T)>,
    ) {
        let (tx, rx) = mpsc::unbounded_channel();
        (
            Self {
                inner: Mutex::new(Inner {
                    tx_reserve: Some(tx.clone()),
                    // The initial Sender created in channel().
                    external_tx_count: 1,
                }),
                sem: Arc::new(Semaphore::new(buffer)),
            },
            tx,
            rx,
        )
    }
}

struct Inner<T>
where
    T: Send + 'static,
{
    tx_reserve: Option<mpsc::UnboundedSender<(Option<OwnedSemaphorePermit>, T)>>,
    external_tx_count: usize,
}

pub struct Permit<'a, T>
where
    T: Send + 'static,
{
    sender: &'a Sender<T>,
    permit: OwnedSemaphorePermit,
}

impl<'a, T> Permit<'a, T>
where
    T: Send + 'static,
{
    pub fn send(self, value: T) -> Result<(), mpsc::error::SendError<T>> {
        match self.sender.tx.send((Some(self.permit), value)) {
            Ok(_) => Ok(()),
            Err(mpsc::error::SendError((_, value))) => Err(mpsc::error::SendError(value)),
        }
    }
}

pub struct OwnedPermit<T>
where
    T: Send + 'static,
{
    sender: Sender<T>,
    permit: OwnedSemaphorePermit,
}

impl<T> OwnedPermit<T>
where
    T: Send + 'static,
{
    pub fn send(self, value: T) -> (Sender<T>, Result<(), mpsc::error::SendError<T>>) {
        let res = self.sender.tx.send((Some(self.permit), value));
        (
            self.sender,
            match res {
                Ok(_) => Ok(()),
                Err(mpsc::error::SendError((_, value))) => Err(mpsc::error::SendError(value)),
            },
        )
    }
}

pub struct Sender<T>
where
    T: Send + 'static,
{
    chan: Arc<Chan<T>>,
    tx: mpsc::UnboundedSender<(Option<OwnedSemaphorePermit>, T)>,
}

impl<T> Sender<T>
where
    T: Send + 'static,
{
    pub async fn reserve(&self) -> Result<Permit<'_, T>, mpsc::error::SendError<()>> {
        match self.chan.sem.clone().acquire_owned().await {
            Ok(permit) => Ok(Permit {
                sender: self,
                permit,
            }),
            Err(_) => Err(mpsc::error::SendError(())),
        }
    }

    pub async fn reserve_owned(self) -> Result<OwnedPermit<T>, mpsc::error::SendError<()>> {
        match self.chan.sem.clone().acquire_owned().await {
            Ok(permit) => Ok(OwnedPermit {
                sender: self,
                permit,
            }),
            Err(_) => Err(mpsc::error::SendError(())),
        }
    }

    pub fn try_reserve(&self) -> Result<Permit<'_, T>, mpsc::error::TrySendError<()>> {
        match self.chan.sem.clone().try_acquire_owned() {
            Ok(permit) => Ok(Permit {
                sender: self,
                permit,
            }),
            Err(_) => Err(mpsc::error::TrySendError::Closed(())),
        }
    }

    pub fn try_reserve_owned(self) -> Result<OwnedPermit<T>, mpsc::error::TrySendError<()>> {
        match self.chan.sem.clone().try_acquire_owned() {
            Ok(permit) => Ok(OwnedPermit {
                sender: self,
                permit,
            }),
            Err(_) => Err(mpsc::error::TrySendError::Closed(())),
        }
    }

    pub async fn send(&self, value: T) -> Result<(), mpsc::error::SendError<T>> {
        match self.reserve().await {
            Ok(permit) => permit.send(value),
            Err(_) => Err(mpsc::error::SendError(value)),
        }
    }

    pub fn try_send(&self, value: T) -> Result<(), mpsc::error::TrySendError<T>> {
        match self.try_reserve() {
            Ok(permit) => match permit.send(value) {
                Ok(_) => Ok(()),
                Err(mpsc::error::SendError(value)) => Err(mpsc::error::TrySendError::Closed(value)),
            },
            Err(mpsc::error::TrySendError::Full(())) => Err(mpsc::error::TrySendError::Full(value)),
            Err(mpsc::error::TrySendError::Closed(())) => {
                Err(mpsc::error::TrySendError::Closed(value))
            }
        }
    }

    pub fn do_send(&self, value: T) -> Result<(), mpsc::error::SendError<T>> {
        match self.tx.send((None, value)) {
            Ok(_) => Ok(()),
            Err(mpsc::error::SendError((_, value))) => Err(mpsc::error::SendError(value)),
        }
    }
}

impl<T> Clone for Sender<T>
where
    T: Send + 'static,
{
    fn clone(&self) -> Self {
        let mut inner = self.chan.inner.lock();
        assert!(
            inner.external_tx_count > 0,
            "Dangling channel while cloning"
        );
        inner.external_tx_count += 1;
        let tx = inner
            .tx_reserve
            .as_ref()
            .expect("Empty tx_reserve, while cloning")
            .clone();
        drop(inner);

        Self {
            chan: self.chan.clone(),
            tx,
        }
    }
}

impl<T> Drop for Sender<T>
where
    T: Send + 'static,
{
    fn drop(&mut self) {
        let mut inner = self.chan.inner.lock();
        assert!(
            inner.external_tx_count > 0,
            "Dangling channel while dropping"
        );
        assert!(
            inner.tx_reserve.is_some(),
            "Empty tx_reserve while dropping"
        );
        inner.external_tx_count -= 1;
        if inner.external_tx_count == 0 {
            inner.tx_reserve.take();
        }
        drop(inner);
    }
}

pub struct Receiver<T>
where
    T: Send + 'static,
{
    chan: Arc<Chan<T>>,
    rx: mpsc::UnboundedReceiver<(Option<OwnedSemaphorePermit>, T)>,
}

impl<T> Receiver<T>
where
    T: Send + 'static,
{
    pub fn sender(&self) -> Option<Sender<T>> {
        let mut inner = self.chan.inner.lock();
        if inner.external_tx_count == 0 {
            None
        } else {
            inner.external_tx_count += 1;
            Some(Sender {
                chan: self.chan.clone(),
                tx: inner
                    .tx_reserve
                    .as_ref()
                    .expect("Dangling tx_reserve")
                    .clone(),
            })
        }
    }
}

pub fn channel<T>(buffer: usize) -> (Sender<T>, Receiver<T>)
where
    T: Send + 'static,
{
    let (chan, tx, rx) = Chan::new(buffer);
    let chan = Arc::new(chan);
    (
        Sender {
            chan: chan.clone(),
            tx,
        },
        Receiver { chan, rx },
    )
}
