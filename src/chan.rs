use parking_lot::Mutex;
use std::sync::Arc;
use tokio::sync::{mpsc, Semaphore};

struct Chan<T>
where
    T: Send + 'static,
{
    inner: Mutex<Inner<T>>,
    sem: Semaphore,
}

impl<T> Chan<T>
where
    T: Send + 'static,
{
    fn new(buffer: usize) -> (Self, mpsc::UnboundedSender<T>, mpsc::UnboundedReceiver<T>) {
        let (tx, rx) = mpsc::unbounded_channel();
        (
            Self {
                inner: Mutex::new(Inner {
                    tx_reserve: Some(tx.clone()),
                    // The initial Sender created in channel().
                    external_tx_count: 1,
                }),
                sem: Semaphore::new(buffer),
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
    tx_reserve: Option<mpsc::UnboundedSender<T>>,
    external_tx_count: usize,
}

pub struct Sender<T>
where
    T: Send + 'static,
{
    chan: Arc<Chan<T>>,
    tx: mpsc::UnboundedSender<T>,
}

impl<T> Clone for Sender<T>
where
    T: Send + 'static,
{
    fn clone(&self) -> Self {
        let mut inner = self.chan.inner.lock();
        assert!(inner.external_tx_count > 0, "Dangling channel while cloning");
        inner.external_tx_count += 1;
        let tx = inner.tx_reserve.as_ref().expect("Empty tx_reserve, while cloning").clone();
        drop(inner);

        Self {
            chan: self.chan.clone(),
            tx,
        }
    }
}

impl<T> Drop for Sender<T>
where
    T: Send + 'static
{
    fn drop(&mut self) {
        let mut inner = self.chan.inner.lock();
        assert!(inner.external_tx_count > 0, "Dangling channel while dropping");
        assert!(inner.tx_reserve.is_some(), "Empty tx_reserve while dropping");
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
    rx: mpsc::UnboundedReceiver<T>,
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
