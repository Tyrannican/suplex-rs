//! Request-Response wrapper over MPMC Crossbeam channels
//!
//! This crate is designed for situations where you want to have request-response channels shared
//! between two or more processes using the [`crossbeam_channel`] crate.
//!
//! # The Left-Right Concept
//!
//! This crate revolves around the concept of a `Left` channel pair and a `Right` channel pair.
//! You select one process to be the `Left` and another process to be the `Right`.
//!
//! The `Left` Process holds a [`Sender<L>`] type and a [`Receiver<R>`] type that sends messages of
//! type `L` (the `Left` process message type) and receives messages of type `R` (the `Right`
//! process message type).
//!
//! The `Right` Process is the inverse which holds a [`Sender<R>`] type and a [`Receiver<L>`] type
//! that sends messages of type `R` (the `Right` process message type) and receives messages of
//! type `L` (the `Left` process message type).
//!
//! # Example Use Case
//!
//! You have a process in a thread that listens for connections and receives messages. Another process in another
//! thread consumes these messages and returns a response back to the connection process which will
//! send it back to the connected client.
//! Here, you could have a channel to send the incoming messages to the Message consumer process
//! and another channel to return the response.
//!
//! This crate allows for you to create these channel pairs so that you can share them between
//! processes easily.
//!
//! # Too Complicated?
//!
//! If the `Left` and `Right` concept is confusing, you don't have to use the [`Bridge`] at all and
//! can create queue pairs with the following functions:
//!
//! * [`unbounded_bridge`] for Unbounded channel pairs
//! * [`bounded_bridge`] for Bounded channel pairs
//! * [`left_bounded_bridge`] for the [`Sender<L>`] and [`Receiver<L>`] to be bound and the [`Sender<R>`] and [`Receiver<R>`] to be unbound
//! * [`right_bounded_bridge`] for the [`Sender<R>`] and [`Receiver<R>`] to be bound and the [`Sender<L>`] and [`Receiver<L>`] to be unbound
//!
//! This allows you to have full control of the channels and you can implement your logic as you
//! please.

pub use crossbeam_channel::{
    Iter, Receiver, RecvError, RecvTimeoutError, SendError, SendTimeoutError, Sender, TryIter,
    TryRecvError, TrySendError,
};
use crossbeam_channel::{bounded, unbounded};
use std::time::{Duration, Instant};

/// Channel split containing the `Left` channels ([`Sender<L>`] & [`Receiver<R>`])
pub type LeftChannelSplit<L, R> = (Sender<L>, Receiver<R>);

/// Channel split containing the `Right` channels ([`Sender<R>`] & [`Receiver<L>`])
pub type RightChannelSplit<L, R> = (Sender<R>, Receiver<L>);

/// The Bridge containing the `Left` and `Right` channels.
///
/// There are two pairs of channels - one with type `L` and the other of type `R`. They act as
/// Request-Response channels that allow for back and forth communication between different processes.
/// This way, you can send data from one side and respond from the other without having to set them
/// up manually.
///
/// For more information on the channels themselves, see the [`crossbeam_channel`] documentation.
#[derive(Debug, Clone)]
pub struct Bridge<L, R> {
    /// The `Left` channels - holding a [`Sender<L>`] and [`Receiver<R>`]
    left: LeftChannelSplit<L, R>,
    /// The `Right` channels - holding a [`Sender<R>`] and [`Receiver<L>`]
    right: RightChannelSplit<L, R>,
}

impl<L, R> Bridge<L, R> {
    /// Create two pairs of channels with no capacity restraints
    ///
    /// # Examples
    ///
    /// ```rust
    /// use std::thread;
    /// use suplex::Bridge;
    ///
    /// let bridge: Bridge<usize, usize> = Bridge::unbounded();
    ///
    /// fn fib(n: usize) -> usize {
    ///     if n <= 1 {
    ///         n
    ///     } else {
    ///         fib(n - 1) + fib(n - 2)
    ///     }
    /// }
    ///
    /// // Sending from the left to the right
    /// let left_sending_bridge = bridge.clone();
    /// thread::spawn(move || left_sending_bridge.send_to_right(fib(20)));
    ///
    /// // Print the result
    /// println!("{}", bridge.recv_from_left().unwrap());
    ///
    /// // Sending from the right to the left
    /// let right_sending_bridge = bridge.clone();
    /// thread::spawn(move || right_sending_bridge.send_to_left(fib(20)));
    ///
    /// // Print the result
    /// println!("{}", bridge.recv_from_right().unwrap());
    /// ```
    pub fn unbounded() -> Self {
        let (left_tx, right_rx) = unbounded::<L>();
        let (right_tx, left_rx) = unbounded::<R>();

        Self {
            left: (left_tx, left_rx),
            right: (right_tx, right_rx),
        }
    }

    /// Create two pairs of channels that can hold at most `cap` messages at a time
    ///
    /// # Examples
    ///
    /// ```rust
    /// use std::thread;
    /// use std::time::Duration;
    /// use suplex::Bridge;
    ///
    /// let bridge: Bridge<usize, usize> = Bridge::bounded(1);
    ///
    /// // Returns immediately as there is enough space in the channel
    /// bridge.send_to_right(1).unwrap();
    ///
    /// let bridge_clone = bridge.clone();
    /// thread::spawn(move || {
    ///     // This blocks the thread as the channel is full
    ///     // It will complete only when the first message is received
    ///     bridge_clone.send_to_right(2);
    /// });
    ///
    /// thread::sleep(Duration::from_secs(1));
    /// assert_eq!(bridge.recv_from_left(), Ok(1));
    /// assert_eq!(bridge.recv_from_left(), Ok(2));
    ///
    /// // The same behaviour applies in the opposite direction (i.e. `Right` -> `Left`)
    /// ```
    pub fn bounded(cap: usize) -> Self {
        let (left_tx, right_rx) = bounded::<L>(cap);
        let (right_tx, left_rx) = bounded::<R>(cap);

        Self {
            left: (left_tx, left_rx),
            right: (right_tx, right_rx),
        }
    }

    /// Create two pairs of channels with the `Left` side being bound to hold `cap` messages at a
    /// time and the `Right` side having no capacity restraints
    ///
    /// # Examples
    ///
    /// ```rust
    /// use std::thread;
    /// use std::time::Duration;
    /// use suplex::Bridge;
    ///
    /// let bridge: Bridge<usize, usize> = Bridge::left_bounded(1);
    ///
    /// // Returns immediately as there is enough space in the channel
    /// bridge.send_to_right(1).unwrap();
    ///
    /// let bridge_clone = bridge.clone();
    /// thread::spawn(move || {
    ///     // This blocks the thread as the channel is full
    ///     // It will complete only when the first message is received
    ///     bridge_clone.send_to_right(2);
    /// });
    ///
    /// thread::sleep(Duration::from_secs(1));
    /// assert_eq!(bridge.recv_from_left(), Ok(1));
    /// assert_eq!(bridge.recv_from_left(), Ok(2));
    ///
    /// for i in 0..5 {
    ///     // This should not block as it is unbounded
    ///     bridge.send_to_left(i).unwrap();
    /// }
    ///
    /// for i in 0..5 {
    ///     // Does not block as unbounded
    ///     assert_eq!(bridge.recv_from_right(), Ok(i));
    /// }
    ///
    /// ```
    pub fn left_bounded(cap: usize) -> Self {
        let (left_tx, right_rx) = bounded::<L>(cap);
        let (right_tx, left_rx) = unbounded::<R>();

        Self {
            left: (left_tx, left_rx),
            right: (right_tx, right_rx),
        }
    }

    /// Create two pairs of channels with the `Right` side being bound to hold `cap` messages at a
    /// time and the `Left` side having no capacity restraints
    ///
    /// # Examples
    ///
    /// ```rust
    /// use std::thread;
    /// use std::time::Duration;
    /// use suplex::Bridge;
    ///
    /// let bridge: Bridge<usize, usize> = Bridge::right_bounded(1);
    ///
    /// // Returns immediately as there is enough space in the channel
    /// bridge.send_to_left(1).unwrap();
    ///
    /// let bridge_clone = bridge.clone();
    /// thread::spawn(move || {
    ///     // This blocks the thread as the channel is full
    ///     // It will complete only when the first message is received
    ///     bridge_clone.send_to_left(2);
    /// });
    ///
    /// thread::sleep(Duration::from_secs(1));
    /// assert_eq!(bridge.recv_from_right(), Ok(1));
    /// assert_eq!(bridge.recv_from_right(), Ok(2));
    ///
    /// for i in 0..5 {
    ///     // This should not block as it is unbounded
    ///     bridge.send_to_right(i).unwrap();
    /// }
    ///
    /// for i in 0..5 {
    ///     // Does not block as unbounded
    ///     assert_eq!(bridge.recv_from_left(), Ok(i));
    /// }
    ///
    /// ```
    pub fn right_bounded(cap: usize) -> Self {
        let (left_tx, right_rx) = unbounded::<L>();
        let (right_tx, left_rx) = bounded::<R>(cap);

        Self {
            left: (left_tx, left_rx),
            right: (right_tx, right_rx),
        }
    }

    /// Send a message from the `Left` channel to the `Right` channel
    ///
    /// # Examples
    ///
    /// ```rust
    /// use std::thread;
    /// use std::time::Duration;
    /// use suplex::Bridge;
    ///
    /// let bridge: Bridge<usize, usize> = Bridge::bounded(1);
    /// assert_eq!(bridge.send_to_right(1), Ok(()));
    ///
    /// let bridge_clone = bridge.clone();
    /// thread::spawn(move || {
    ///     assert_eq!(bridge_clone.recv_from_left(), Ok(1));
    ///     thread::sleep(Duration::from_secs(1));
    ///     drop(bridge_clone);
    /// });
    ///
    /// assert_eq!(bridge.send_to_right(2), Ok(()));
    /// assert_eq!(bridge.recv_from_left(), Ok(2));
    /// ```
    pub fn send_to_right(&self, msg: L) -> Result<(), SendError<L>> {
        self.left.0.send(msg)
    }

    /// Send a message from the `Right` channel to the `Left` channel
    ///
    /// # Examples
    ///
    /// ```rust
    /// use std::thread;
    /// use std::time::Duration;
    /// use suplex::Bridge;
    ///
    /// let bridge: Bridge<usize, usize> = Bridge::bounded(1);
    /// assert_eq!(bridge.send_to_left(1), Ok(()));
    ///
    /// let bridge_clone = bridge.clone();
    /// thread::spawn(move || {
    ///     assert_eq!(bridge_clone.recv_from_right(), Ok(1));
    ///     thread::sleep(Duration::from_secs(1));
    ///     drop(bridge_clone);
    /// });
    ///
    /// assert_eq!(bridge.send_to_left(2), Ok(()));
    /// assert_eq!(bridge.recv_from_right(), Ok(2));
    /// ```
    pub fn send_to_left(&self, msg: R) -> Result<(), SendError<R>> {
        self.right.0.send(msg)
    }

    /// Attempts to send a message from the `Left` channel to the `Right` channel without blocking
    ///
    /// # Examples
    ///
    /// ```rust
    /// use suplex::{Bridge, TrySendError};
    ///
    /// let bridge: Bridge<usize, usize> = Bridge::bounded(1);
    ///
    /// assert_eq!(bridge.try_send_to_right(1), Ok(()));
    /// assert_eq!(bridge.try_send_to_right(2), Err(TrySendError::Full(2)));
    /// ```
    pub fn try_send_to_right(&self, msg: L) -> Result<(), TrySendError<L>> {
        self.left.0.try_send(msg)
    }

    /// Attempts to send a message from the `Right` channel to the `Left` channel without blocking
    ///
    /// # Examples
    ///
    /// ```rust
    /// use suplex::{Bridge, TrySendError};
    ///
    /// let bridge: Bridge<usize, usize> = Bridge::bounded(1);
    ///
    /// assert_eq!(bridge.try_send_to_left(1), Ok(()));
    /// assert_eq!(bridge.try_send_to_left(2), Err(TrySendError::Full(2)));
    /// ```
    pub fn try_send_to_left(&self, msg: R) -> Result<(), TrySendError<R>> {
        self.right.0.try_send(msg)
    }

    /// Waits for a message to be sent into the `Left` channel to the `Right` channel for a limited
    /// time
    ///
    /// # Examples
    ///
    /// ```rust
    /// use std::thread;
    /// use std::time::Duration;
    /// use suplex::{Bridge, SendTimeoutError};
    ///
    /// let bridge: Bridge<usize, usize> = Bridge::bounded(0);
    ///
    /// let bridge_clone = bridge.clone();
    /// thread::spawn(move || {
    ///     thread::sleep(Duration::from_secs(1));
    ///     assert_eq!(bridge_clone.recv_from_left(), Ok(2));
    ///     drop(bridge_clone);
    /// });
    ///
    /// assert_eq!(
    ///     bridge.send_to_right_timeout(1, Duration::from_millis(500)),
    ///     Err(SendTimeoutError::Timeout(1)),
    /// );
    ///
    /// assert_eq!(
    ///     bridge.send_to_right_timeout(2, Duration::from_secs(1)),
    ///     Ok(())
    /// );
    /// ```
    pub fn send_to_right_timeout(
        &self,
        msg: L,
        timeout: Duration,
    ) -> Result<(), SendTimeoutError<L>> {
        self.left.0.send_timeout(msg, timeout)
    }

    /// Waits for a message to be sent into the `Right` channel to the `Left` channel for a limited
    /// time
    ///
    /// # Examples
    ///
    /// ```rust
    /// use std::thread;
    /// use std::time::Duration;
    /// use suplex::{Bridge, SendTimeoutError};
    ///
    /// let bridge: Bridge<usize, usize> = Bridge::bounded(0);
    ///
    /// let bridge_clone = bridge.clone();
    /// thread::spawn(move || {
    ///     thread::sleep(Duration::from_secs(1));
    ///     assert_eq!(bridge_clone.recv_from_right(), Ok(2));
    ///     drop(bridge_clone);
    /// });
    ///
    /// assert_eq!(
    ///     bridge.send_to_left_timeout(1, Duration::from_millis(500)),
    ///     Err(SendTimeoutError::Timeout(1)),
    /// );
    ///
    /// assert_eq!(
    ///     bridge.send_to_left_timeout(2, Duration::from_secs(1)),
    ///     Ok(())
    /// );
    /// ```
    pub fn send_to_left_timeout(
        &self,
        msg: R,
        timeout: Duration,
    ) -> Result<(), SendTimeoutError<R>> {
        self.right.0.send_timeout(msg, timeout)
    }

    /// Waits for a message to be sent into the `Left` channel to the `Right` channel until a given
    /// deadline
    ///
    /// # Examples
    ///
    /// ```rust
    /// use std::thread;
    /// use std::time::{Duration, Instant};
    /// use suplex::{Bridge, SendTimeoutError};
    ///
    /// let bridge: Bridge<usize, usize> = Bridge::bounded(0);
    /// let bridge_clone = bridge.clone();
    ///
    /// thread::spawn(move || {
    ///     thread::sleep(Duration::from_secs(1));
    ///     assert_eq!(bridge_clone.recv_from_left(), Ok(2));
    ///     drop(bridge_clone);
    /// });
    ///
    /// let now = Instant::now();
    ///
    /// assert_eq!(
    ///     bridge.send_to_right_deadline(1, now + Duration::from_millis(500)),
    ///     Err(SendTimeoutError::Timeout(1)),
    /// );
    ///
    /// assert_eq!(
    ///     bridge.send_to_right_deadline(2, now + Duration::from_millis(1500)),
    ///     Ok(())
    /// );
    ///
    /// ```
    pub fn send_to_right_deadline(
        &self,
        msg: L,
        deadline: Instant,
    ) -> Result<(), SendTimeoutError<L>> {
        self.left.0.send_deadline(msg, deadline)
    }

    /// Waits for a message to be sent into the `Right` channel to the `Left` channel until a given
    /// deadline
    ///
    /// # Examples
    ///
    /// ```rust
    /// use std::thread;
    /// use std::time::{Duration, Instant};
    /// use suplex::{Bridge, SendTimeoutError};
    ///
    /// let bridge: Bridge<usize, usize> = Bridge::bounded(0);
    /// let bridge_clone = bridge.clone();
    ///
    /// thread::spawn(move || {
    ///     thread::sleep(Duration::from_secs(1));
    ///     assert_eq!(bridge_clone.recv_from_right(), Ok(2));
    ///     drop(bridge_clone);
    /// });
    ///
    /// let now = Instant::now();
    ///
    /// assert_eq!(
    ///     bridge.send_to_left_deadline(1, now + Duration::from_millis(500)),
    ///     Err(SendTimeoutError::Timeout(1)),
    /// );
    ///
    /// assert_eq!(
    ///     bridge.send_to_left_deadline(2, now + Duration::from_millis(1500)),
    ///     Ok(())
    /// );
    ///
    /// ```
    pub fn send_to_left_deadline(
        &self,
        msg: R,
        deadline: Instant,
    ) -> Result<(), SendTimeoutError<R>> {
        self.right.0.send_deadline(msg, deadline)
    }

    /// Receive a message sent from the `Left` channel on the `Right` channel
    ///
    /// # Examples
    ///
    /// ```rust
    /// use std::thread;
    /// use std::time::Duration;
    /// use suplex::{Bridge, RecvError};
    ///
    /// let bridge: Bridge<usize, usize> = Bridge::unbounded();
    /// let bridge_clone = bridge.clone();
    ///
    /// thread::spawn(move || {
    ///     thread::sleep(Duration::from_secs(1));
    ///     bridge_clone.send_to_right(5).unwrap();
    ///     drop(bridge_clone);
    /// });
    ///
    /// assert_eq!(bridge.recv_from_left(), Ok(5));
    /// ```
    pub fn recv_from_left(&self) -> Result<L, RecvError> {
        self.right.1.recv()
    }

    /// Receive a message sent from the `Right` channel on the `Left` channel
    ///
    /// # Examples
    ///
    /// ```rust
    /// use std::thread;
    /// use std::time::Duration;
    /// use suplex::{Bridge, RecvError};
    ///
    /// let bridge: Bridge<usize, usize> = Bridge::unbounded();
    /// let bridge_clone = bridge.clone();
    ///
    /// thread::spawn(move || {
    ///     thread::sleep(Duration::from_secs(1));
    ///     bridge_clone.send_to_left(5).unwrap();
    ///     drop(bridge_clone);
    /// });
    ///
    /// assert_eq!(bridge.recv_from_right(), Ok(5));
    /// ```
    pub fn recv_from_right(&self) -> Result<R, RecvError> {
        self.left.1.recv()
    }

    /// Attempts to receive a message sent from the `Left` channel on the `Right` channel without
    /// blocking
    ///
    /// # Examples
    ///
    /// ```rust
    /// use suplex::{Bridge, TryRecvError};
    ///
    /// let bridge: Bridge<usize, usize> = Bridge::unbounded();
    /// assert_eq!(bridge.try_recv_from_left(), Err(TryRecvError::Empty));
    ///
    /// bridge.send_to_right(5).unwrap();
    /// assert_eq!(bridge.try_recv_from_left(), Ok(5));
    /// ```
    pub fn try_recv_from_left(&self) -> Result<L, TryRecvError> {
        self.right.1.try_recv()
    }

    /// Attempts to receive a message sent from the `Right` channel on the `Left` channel without
    /// blocking
    ///
    /// # Examples
    ///
    /// ```rust
    /// use suplex::{Bridge, TryRecvError};
    ///
    /// let bridge: Bridge<usize, usize> = Bridge::unbounded();
    /// assert_eq!(bridge.try_recv_from_right(), Err(TryRecvError::Empty));
    ///
    /// bridge.send_to_left(5).unwrap();
    /// assert_eq!(bridge.try_recv_from_right(), Ok(5));
    /// ```
    pub fn try_recv_from_right(&self) -> Result<R, TryRecvError> {
        self.left.1.try_recv()
    }

    /// Waits for a message to be received on the `Right` channel from the `Left` channel for a
    /// limited time
    ///
    /// # Examples
    ///
    /// ```rust
    /// use std::thread;
    /// use std::time::Duration;
    /// use suplex::{Bridge, RecvTimeoutError};
    ///
    /// let bridge: Bridge<usize, usize> = Bridge::unbounded();
    /// let bridge_clone = bridge.clone();
    ///
    /// thread::spawn(move || {
    ///     thread::sleep(Duration::from_secs(1));
    ///     bridge_clone.send_to_right(5).unwrap();
    ///     drop(bridge_clone);
    /// });
    ///
    /// assert_eq!(
    ///     bridge.recv_from_left_timeout(Duration::from_millis(500)),
    ///     Err(RecvTimeoutError::Timeout),
    /// );
    ///
    /// assert_eq!(
    ///     bridge.recv_from_left_timeout(Duration::from_secs(1)),
    ///     Ok(5)
    /// );
    /// ```
    pub fn recv_from_left_timeout(&self, timeout: Duration) -> Result<L, RecvTimeoutError> {
        self.right.1.recv_timeout(timeout)
    }

    /// Waits for a message to be received on the `Left` channel from the `Right` channel for a
    /// limited time
    ///
    /// # Examples
    ///
    /// ```rust
    /// use std::thread;
    /// use std::time::Duration;
    /// use suplex::{Bridge, RecvTimeoutError};
    ///
    /// let bridge: Bridge<usize, usize> = Bridge::unbounded();
    /// let bridge_clone = bridge.clone();
    ///
    /// thread::spawn(move || {
    ///     thread::sleep(Duration::from_secs(1));
    ///     bridge_clone.send_to_left(5).unwrap();
    ///     drop(bridge_clone);
    /// });
    ///
    /// assert_eq!(
    ///     bridge.recv_from_right_timeout(Duration::from_millis(500)),
    ///     Err(RecvTimeoutError::Timeout),
    /// );
    ///
    /// assert_eq!(
    ///     bridge.recv_from_right_timeout(Duration::from_secs(1)),
    ///     Ok(5)
    /// );
    /// ```
    pub fn recv_from_right_timeout(&self, timeout: Duration) -> Result<R, RecvTimeoutError> {
        self.left.1.recv_timeout(timeout)
    }

    /// Waits for a message to be received on the `Right` channel from the `Left` channel until a
    /// given deadline
    ///
    /// # Examples
    ///
    /// ```rust
    /// use std::thread;
    /// use std::time::{Instant, Duration};
    /// use suplex::{Bridge, RecvTimeoutError};
    ///
    /// let bridge: Bridge<usize, usize> = Bridge::unbounded();
    /// let bridge_clone = bridge.clone();
    ///
    /// thread::spawn(move || {
    ///     thread::sleep(Duration::from_secs(1));
    ///     bridge_clone.send_to_right(5).unwrap();
    ///     drop(bridge_clone);
    /// });
    ///
    /// let now = Instant::now();
    ///
    /// assert_eq!(
    ///     bridge.recv_from_left_deadline(now + Duration::from_millis(500)),
    ///     Err(RecvTimeoutError::Timeout),
    /// );
    ///
    /// assert_eq!(
    ///     bridge.recv_from_left_deadline(now + Duration::from_millis(1500)),
    ///     Ok(5)
    /// );
    /// ```
    pub fn recv_from_left_deadline(&self, deadline: Instant) -> Result<L, RecvTimeoutError> {
        self.right.1.recv_deadline(deadline)
    }

    /// Waits for a message to be received on the `Left` channel from the `Right` channel until a
    /// given deadline
    ///
    /// # Examples
    ///
    /// ```rust
    /// use std::thread;
    /// use std::time::{Instant, Duration};
    /// use suplex::{Bridge, RecvTimeoutError};
    ///
    /// let bridge: Bridge<usize, usize> = Bridge::unbounded();
    /// let bridge_clone = bridge.clone();
    ///
    /// thread::spawn(move || {
    ///     thread::sleep(Duration::from_secs(1));
    ///     bridge_clone.send_to_left(5).unwrap();
    ///     drop(bridge_clone);
    /// });
    ///
    /// let now = Instant::now();
    ///
    /// assert_eq!(
    ///     bridge.recv_from_right_deadline(now + Duration::from_millis(500)),
    ///     Err(RecvTimeoutError::Timeout),
    /// );
    ///
    /// assert_eq!(
    ///     bridge.recv_from_right_deadline(now + Duration::from_millis(1500)),
    ///     Ok(5)
    /// );
    /// ```
    pub fn recv_from_right_deadline(&self, deadline: Instant) -> Result<R, RecvTimeoutError> {
        self.left.1.recv_deadline(deadline)
    }

    /// A blocking iterator over messages in the `Left` receiver channel (i.e. iterator over the
    /// `R` messages received by the `Left`)
    ///
    /// # Examples
    ///
    /// ```rust
    /// use std::thread;
    /// use suplex::Bridge;
    ///
    /// let bridge: Bridge<usize, usize> = Bridge::unbounded();
    /// let bridge_clone = bridge.clone();
    ///
    /// thread::spawn(move || {
    ///     bridge_clone.send_to_left(1).unwrap();
    ///     bridge_clone.send_to_left(2).unwrap();
    ///     bridge_clone.send_to_left(3).unwrap();
    ///     drop(bridge_clone);
    /// });
    ///
    /// let left_receiver_iter = bridge.left_receiver_iter();
    ///
    /// // Do what you want with the iter (this will endlessly block);
    /// ```
    pub fn left_receiver_iter(&self) -> Iter<'_, R> {
        self.left.1.iter()
    }

    /// A blocking iterator over messages in the `Right` receiver channel (i.e. iterator over the
    /// `L` messages received by the `Right`)
    ///
    /// # Examples
    ///
    /// ```rust
    /// use std::thread;
    /// use suplex::Bridge;
    ///
    /// let bridge: Bridge<usize, usize> = Bridge::unbounded();
    /// let bridge_clone = bridge.clone();
    ///
    /// thread::spawn(move || {
    ///     bridge_clone.send_to_right(1).unwrap();
    ///     bridge_clone.send_to_right(2).unwrap();
    ///     bridge_clone.send_to_right(3).unwrap();
    ///     drop(bridge_clone);
    /// });
    ///
    /// let right_receiver_iter = bridge.right_receiver_iter();
    /// // Do what you want with the iter (this will endlessly block);
    /// ```
    pub fn right_receiver_iter(&self) -> Iter<'_, L> {
        self.right.1.iter()
    }

    /// A non-blocking iterator over messages in the `Left` receiver channel (i.e. iterator over
    /// the `R` messages received by the `Left`)
    ///
    /// Each call to `next` returns a message if there is one ready to be received. This never
    /// blocks waiting for the next message
    ///
    /// # Examples
    ///
    /// ```rust
    /// use std::thread;
    /// use std::time::Duration;
    /// use suplex::Bridge;
    ///
    /// let bridge: Bridge<usize, usize> = Bridge::unbounded();
    /// let bridge_clone = bridge.clone();
    ///
    /// thread::spawn(move || {
    ///     bridge_clone.send_to_left(1).unwrap();
    ///     thread::sleep(Duration::from_secs(1));
    ///     bridge_clone.send_to_left(2).unwrap();
    ///     thread::sleep(Duration::from_secs(2));
    ///     bridge_clone.send_to_left(3).unwrap();
    ///     drop(bridge_clone);
    /// });
    ///
    /// thread::sleep(Duration::from_secs(2));
    ///
    /// // Collect all messages from the channel without blocking.
    /// // The third message hasn't been sent tyet so we'll collect only the first one
    /// let v: Vec<_> = bridge.left_receiver_try_iter().collect();
    ///
    /// assert_eq!(v, [1, 2]);
    /// ```
    pub fn left_receiver_try_iter(&self) -> TryIter<'_, R> {
        self.left.1.try_iter()
    }

    /// A non-blocking iterator over messages in the `Right` receiver channel (i.e. iterator over
    /// the `R` messages send by the `Left`)
    ///
    /// Each call to `next` returns a message if there is one ready to be received. This never
    /// blocks waiting for the next message
    ///
    /// # Examples
    ///
    /// ```rust
    /// use std::thread;
    /// use std::time::Duration;
    /// use suplex::Bridge;
    ///
    /// let bridge: Bridge<usize, usize> = Bridge::unbounded();
    /// let bridge_clone = bridge.clone();
    ///
    /// thread::spawn(move || {
    ///     bridge_clone.send_to_right(1).unwrap();
    ///     thread::sleep(Duration::from_secs(1));
    ///     bridge_clone.send_to_right(2).unwrap();
    ///     thread::sleep(Duration::from_secs(2));
    ///     bridge_clone.send_to_right(3).unwrap();
    ///     drop(bridge_clone);
    /// });
    ///
    /// thread::sleep(Duration::from_secs(2));
    ///
    /// // Collect all messages from the channel without blocking.
    /// // The third message hasn't been sent tyet so we'll collect only the first one
    /// let v: Vec<_> = bridge.right_receiver_try_iter().collect();
    ///
    /// assert_eq!(v, [1, 2]);
    /// ```
    pub fn right_receiver_try_iter(&self) -> TryIter<'_, L> {
        self.right.1.try_iter()
    }

    /// The capacity of the `Left` sender channel
    ///
    /// Returns [`None`] if the channel is unbounded
    ///
    /// # Examples
    ///
    /// ```rust
    /// use suplex::Bridge;
    ///
    /// let bridge: Bridge<usize, usize> = Bridge::unbounded();
    /// assert_eq!(bridge.left_sender_capacity(), None);
    ///
    /// let bridge: Bridge<usize, usize> = Bridge::bounded(5);
    /// assert_eq!(bridge.left_sender_capacity(), Some(5));
    ///
    /// let bridge: Bridge<usize, usize> = Bridge::bounded(0);
    /// assert_eq!(bridge.left_sender_capacity(), Some(0));
    /// ```
    pub fn left_sender_capacity(&self) -> Option<usize> {
        self.left.0.capacity()
    }

    /// The capacity of the `Right` sender channel
    ///
    /// Returns [`None`] if the channel is unbounded
    ///
    /// # Examples
    ///
    /// ```rust
    /// use suplex::Bridge;
    ///
    /// let bridge: Bridge<usize, usize> = Bridge::unbounded();
    /// assert_eq!(bridge.right_sender_capacity(), None);
    ///
    /// let bridge: Bridge<usize, usize> = Bridge::bounded(5);
    /// assert_eq!(bridge.right_sender_capacity(), Some(5));
    ///
    /// let bridge: Bridge<usize, usize> = Bridge::bounded(0);
    /// assert_eq!(bridge.right_sender_capacity(), Some(0));
    /// ```
    pub fn right_sender_capacity(&self) -> Option<usize> {
        self.right.0.capacity()
    }

    /// Returns `true` if the `Left` sender channel is empty
    ///
    /// # Examples
    ///
    /// ```rust
    /// use suplex::Bridge;
    /// let bridge: Bridge<usize, usize> = Bridge::unbounded();
    /// assert!(bridge.is_left_sender_empty());
    ///
    /// bridge.send_to_right(0).unwrap();
    /// assert!(!bridge.is_left_sender_empty());
    /// ```
    pub fn is_left_sender_empty(&self) -> bool {
        self.left.0.is_empty()
    }

    /// Returns `true` if the `Right` sender channel is empty
    ///
    /// # Examples
    ///
    /// ```rust
    /// use suplex::Bridge;
    /// let bridge: Bridge<usize, usize> = Bridge::unbounded();
    /// assert!(bridge.is_right_sender_empty());
    ///
    /// bridge.send_to_left(0).unwrap();
    /// assert!(!bridge.is_right_sender_empty());
    /// ```
    pub fn is_right_sender_empty(&self) -> bool {
        self.right.0.is_empty()
    }

    /// Returns `true` if the `Left` sender channel is full
    ///
    /// # Examples
    ///
    /// ```rust
    /// use suplex::Bridge;
    ///
    /// let bridge: Bridge<usize, usize> = Bridge::bounded(1);
    /// assert!(!bridge.is_left_sender_full());
    ///
    /// bridge.send_to_right(0).unwrap();
    /// assert!(bridge.is_left_sender_full());
    /// ```
    pub fn is_left_sender_full(&self) -> bool {
        self.left.0.is_full()
    }

    /// Returns `true` if the `Right` sender channel is full
    ///
    /// # Examples
    ///
    /// ```rust
    /// use suplex::Bridge;
    ///
    /// let bridge: Bridge<usize, usize> = Bridge::bounded(1);
    /// assert!(!bridge.is_right_sender_full());
    ///
    /// bridge.send_to_left(0).unwrap();
    /// assert!(bridge.is_right_sender_full());
    /// ```
    pub fn is_right_sender_full(&self) -> bool {
        self.right.0.is_full()
    }

    /// Returns the number of message in the `Left` sender channel
    ///
    /// # Examples
    ///
    /// ```rust
    /// use suplex::Bridge;
    ///
    /// let bridge: Bridge<usize, usize> = Bridge::unbounded();
    /// assert_eq!(bridge.left_sender_len(), 0);
    ///
    /// bridge.send_to_right(1).unwrap();
    /// bridge.send_to_right(2).unwrap();
    ///
    /// assert_eq!(bridge.left_sender_len(), 2);
    /// ```
    pub fn left_sender_len(&self) -> usize {
        self.left.0.len()
    }

    /// Returns the number of message in the `Right` sender channel
    ///
    /// # Examples
    ///
    /// ```rust
    /// use suplex::Bridge;
    ///
    /// let bridge: Bridge<usize, usize> = Bridge::unbounded();
    /// assert_eq!(bridge.right_sender_len(), 0);
    ///
    /// bridge.send_to_left(1).unwrap();
    /// bridge.send_to_left(2).unwrap();
    ///
    /// assert_eq!(bridge.right_sender_len(), 2);
    /// ```
    pub fn right_sender_len(&self) -> usize {
        self.right.0.len()
    }

    /// Returns the capacity of the `Left` receiver channel
    ///
    /// Returns [`None`] if the channel is unbounded
    ///
    /// # Examples
    ///
    /// ```rust
    /// use suplex::Bridge;
    ///
    /// let bridge: Bridge<usize, usize> = Bridge::unbounded();
    /// assert_eq!(bridge.left_receiver_capacity(), None);
    ///
    /// let bridge: Bridge<usize, usize> = Bridge::bounded(5);
    /// assert_eq!(bridge.left_receiver_capacity(), Some(5));
    ///
    /// let bridge: Bridge<usize, usize> = Bridge::bounded(0);
    /// assert_eq!(bridge.left_receiver_capacity(), Some(0));
    /// ```
    pub fn left_receiver_capacity(&self) -> Option<usize> {
        self.right.1.capacity()
    }

    /// Returns the capacity of the `Right` receiver channel
    ///
    /// Returns [`None`] if the channel is unbounded
    ///
    /// # Examples
    ///
    /// ```rust
    /// use suplex::Bridge;
    ///
    /// let bridge: Bridge<usize, usize> = Bridge::unbounded();
    /// assert_eq!(bridge.right_receiver_capacity(), None);
    ///
    /// let bridge: Bridge<usize, usize> = Bridge::bounded(5);
    /// assert_eq!(bridge.right_receiver_capacity(), Some(5));
    ///
    /// let bridge: Bridge<usize, usize> = Bridge::bounded(0);
    /// assert_eq!(bridge.right_receiver_capacity(), Some(0));
    /// ```
    pub fn right_receiver_capacity(&self) -> Option<usize> {
        self.left.1.capacity()
    }

    /// Returns the number of messages in the `Left` receiver channel
    ///
    /// # Examples
    ///
    /// ```rust
    /// use suplex::Bridge;
    ///
    /// let bridge: Bridge<usize, usize> = Bridge::unbounded();
    /// assert_eq!(bridge.left_receiver_len(), 0);
    ///
    /// bridge.send_to_right(1).unwrap();
    /// bridge.send_to_right(2).unwrap();
    /// assert_eq!(bridge.left_receiver_len(), 2);
    /// ```
    pub fn left_receiver_len(&self) -> usize {
        self.right.1.len()
    }

    /// Returns the number of messages in the `Right` receiver channel
    ///
    /// # Examples
    ///
    /// ```rust
    /// use suplex::Bridge;
    ///
    /// let bridge: Bridge<usize, usize> = Bridge::unbounded();
    /// assert_eq!(bridge.right_receiver_len(), 0);
    ///
    /// bridge.send_to_left(1).unwrap();
    /// bridge.send_to_left(2).unwrap();
    /// assert_eq!(bridge.right_receiver_len(), 2);
    /// ```
    pub fn right_receiver_len(&self) -> usize {
        self.left.1.len()
    }

    /// Returns `true` if the `Left` receiver channel is empty
    ///
    /// # Examples
    ///
    /// ```rust
    /// use suplex::Bridge;
    ///
    /// let bridge: Bridge<usize, usize> = Bridge::unbounded();
    /// assert!(bridge.is_left_receiver_empty());
    ///
    /// bridge.send_to_right(0).unwrap();
    /// assert!(!bridge.is_left_receiver_empty());
    /// ```
    pub fn is_left_receiver_empty(&self) -> bool {
        self.right.1.is_empty()
    }

    /// Returns `true` if the `Right` receiver channel is empty
    ///
    /// # Examples
    ///
    /// ```rust
    /// use suplex::Bridge;
    ///
    /// let bridge: Bridge<usize, usize> = Bridge::unbounded();
    /// assert!(bridge.is_right_receiver_empty());
    ///
    /// bridge.send_to_left(0).unwrap();
    /// assert!(!bridge.is_right_receiver_empty());
    /// ```
    pub fn is_right_receiver_empty(&self) -> bool {
        self.left.1.is_empty()
    }

    /// Returns `true` if the `Left` receiver channel is full
    ///
    /// # Examples
    ///
    /// ```rust
    /// use suplex::Bridge;
    ///
    /// let bridge: Bridge<usize, usize> = Bridge::bounded(1);
    /// assert!(!bridge.is_left_receiver_full());
    ///
    /// bridge.send_to_right(0).unwrap();
    /// assert!(bridge.is_left_receiver_full());
    /// ```
    pub fn is_left_receiver_full(&self) -> bool {
        self.right.1.is_full()
    }

    /// Returns `true` if the `Right` receiver channel is full
    ///
    /// # Examples
    ///
    /// ```rust
    /// use suplex::Bridge;
    ///
    /// let bridge: Bridge<usize, usize> = Bridge::bounded(1);
    /// assert!(!bridge.is_right_receiver_full());
    ///
    /// bridge.send_to_left(0).unwrap();
    /// assert!(bridge.is_right_receiver_full());
    /// ```
    pub fn is_right_receiver_full(&self) -> bool {
        self.left.1.is_full()
    }

    /// Returns `true` if the given sender belongs to the `Left` sender channel
    ///
    /// # Examples
    ///
    /// ```rust
    /// use suplex::Bridge;
    ///
    /// let bridge: Bridge<usize, usize> = Bridge::unbounded();
    /// let left_sender = bridge.left_sender();
    /// assert!(bridge.same_channel_left_sender(&left_sender));
    ///
    /// let another_bridge: Bridge<usize, usize> = Bridge::unbounded();
    /// assert!(!another_bridge.same_channel_left_sender(&left_sender));
    /// ```
    pub fn same_channel_left_sender(&self, other: &Sender<L>) -> bool {
        self.left.0.same_channel(other)
    }

    /// Returns `true` if the given sender belongs to the `Right` sender channel
    ///
    /// # Examples
    ///
    /// ```rust
    /// use suplex::Bridge;
    ///
    /// let bridge: Bridge<usize, usize> = Bridge::unbounded();
    /// let right_sender = bridge.right_sender();
    /// assert!(bridge.same_channel_right_sender(&right_sender));
    ///
    /// let another_bridge: Bridge<usize, usize> = Bridge::unbounded();
    /// assert!(!another_bridge.same_channel_right_sender(&right_sender));
    /// ```
    pub fn same_channel_right_sender(&self, other: &Sender<R>) -> bool {
        self.right.0.same_channel(other)
    }

    /// Returns `true` if the given receiver belongs to the `Left` receiver channel
    ///
    /// # Examples
    ///
    /// ```rust
    /// use suplex::Bridge;
    ///
    /// let bridge: Bridge<usize, usize> = Bridge::unbounded();
    /// let left_receiver = bridge.left_receiver();
    /// assert!(bridge.same_channel_left_receiver(&left_receiver));
    ///
    /// let another_bridge: Bridge<usize, usize> = Bridge::unbounded();
    /// assert!(!another_bridge.same_channel_left_receiver(&left_receiver));
    /// ```
    pub fn same_channel_left_receiver(&self, other: &Receiver<R>) -> bool {
        self.left.1.same_channel(other)
    }

    /// Returns `true` if the given receiver belongs to the `Right` receiver channel
    ///
    /// # Examples
    ///
    /// ```rust
    /// use suplex::Bridge;
    ///
    /// let bridge: Bridge<usize, usize> = Bridge::unbounded();
    /// let right_receiver = bridge.right_receiver();
    /// assert!(bridge.same_channel_right_receiver(&right_receiver));
    ///
    /// let another_bridge: Bridge<usize, usize> = Bridge::unbounded();
    /// assert!(!another_bridge.same_channel_right_receiver(&right_receiver));
    /// ```
    pub fn same_channel_right_receiver(&self, other: &Receiver<L>) -> bool {
        self.right.1.same_channel(other)
    }

    /// Returns a clone of the sender on the `Left`
    ///
    /// # Examples
    ///
    /// ```rust
    /// use suplex::Bridge;
    ///
    /// let bridge: Bridge<usize, usize> = Bridge::unbounded();
    /// let left_sender = bridge.left_sender();
    /// assert!(bridge.same_channel_left_sender(&left_sender));
    /// ```
    pub fn left_sender(&self) -> Sender<L> {
        self.left.0.clone()
    }

    /// Returns a clone of the sender on the `Right`
    ///
    /// # Examples
    ///
    /// ```rust
    /// use suplex::Bridge;
    ///
    /// let bridge: Bridge<usize, usize> = Bridge::unbounded();
    /// let right_sender = bridge.right_sender();
    /// assert!(bridge.same_channel_right_sender(&right_sender));
    /// ```
    pub fn right_sender(&self) -> Sender<R> {
        self.right.0.clone()
    }

    /// Returns a clone of the receiver on the `Left`
    ///
    /// # Examples
    ///
    /// ```rust
    /// use suplex::Bridge;
    ///
    /// let bridge: Bridge<usize, usize> = Bridge::unbounded();
    /// let left_receiver = bridge.left_receiver();
    /// assert!(bridge.same_channel_left_receiver(&left_receiver));
    /// ```
    pub fn left_receiver(&self) -> Receiver<R> {
        self.left.1.clone()
    }

    /// Returns a clone of the receiver on the `Right`
    ///
    /// # Examples
    ///
    /// ```rust
    /// use suplex::Bridge;
    ///
    /// let bridge: Bridge<usize, usize> = Bridge::unbounded();
    /// let right_receiver = bridge.right_receiver();
    /// assert!(bridge.same_channel_right_receiver(&right_receiver));
    /// ```
    pub fn right_receiver(&self) -> Receiver<L> {
        self.right.1.clone()
    }
}

/// Creates two pairs of unbounded channels for the `Left` and `Right` types
///
/// This returns a [`LeftChannelSplit<L, R>`] and [`RightChannelSplit<L, R>`] which hold a sender
/// and a receiver of the opposite type
///
/// # Examples
///
/// ```rust
/// use std::thread;
/// use suplex::unbounded_bridge;
///
/// let (left, right) = unbounded_bridge::<usize, usize>();
///
/// // The `Right` process
/// thread::spawn(move || {
///     let (right_sender, left_receiver) = right;
///     assert_eq!(left_receiver.recv(), Ok(1));
///     right_sender.send(5).unwrap();
/// });
///
/// // The `Left` process
/// let (left_sender, right_receiver) = left;
/// left_sender.send(1).unwrap();
/// assert_eq!(right_receiver.recv(), Ok(5));
/// ```
pub fn unbounded_bridge<L, R>() -> (LeftChannelSplit<L, R>, RightChannelSplit<L, R>) {
    let (left_tx, right_rx) = unbounded::<L>();
    let (right_tx, left_rx) = unbounded::<R>();

    ((left_tx, left_rx), (right_tx, right_rx))
}

/// Creates two pairs of bounded channels for the `Left` and `Right` types
///
/// The [`Sender<L>`] and [`Receiver<L>`] are bound by the `left_cap`
/// The [`Sender<R>`] and [`Receiver<R>`] are bound by the `right_cap`
///
/// This returns a [`LeftChannelSplit<L, R>`] and [`RightChannelSplit<L, R>`] which hold a sender
/// and a receiver of the opposite type
///
/// # Examples
///
/// ```rust
/// use std::thread;
/// use suplex::bounded_bridge;
///
/// let (left, right) = bounded_bridge::<usize, usize>(1, 1);
///
/// // The `Right` process
/// thread::spawn(move || {
///     let (right_sender, left_receiver) = right;
///     assert_eq!(left_receiver.recv(), Ok(1));
///     right_sender.send(5).unwrap();
/// });
///
/// // The `Left` process
/// let (left_sender, right_receiver) = left;
/// left_sender.send(1).unwrap();
/// assert_eq!(right_receiver.recv(), Ok(5));
/// ```
pub fn bounded_bridge<L, R>(
    left_cap: usize,
    right_cap: usize,
) -> (LeftChannelSplit<L, R>, RightChannelSplit<L, R>) {
    let (left_tx, right_rx) = bounded::<L>(left_cap);
    let (right_tx, left_rx) = bounded::<R>(right_cap);

    ((left_tx, left_rx), (right_tx, right_rx))
}

/// Creates two pairs of channels for the `Left` and `Right` types with the `Left` side bounded.
///
/// The [`Sender<L>`] and [`Receiver<L>`] are bound by the `cap` and the [`Sender<R>`] and
/// [`Receiver<R>`] is unbound
///
/// This returns a [`LeftChannelSplit<L, R>`] and [`RightChannelSplit<L, R>`] which hold a sender
/// and a receiver of the opposite type
///
/// # Examples
///
/// ```rust
/// use std::thread;
/// use suplex::left_bounded_bridge;
///
/// let (left, right) = left_bounded_bridge::<usize, usize>(1);
///
/// // The `Right` process
/// thread::spawn(move || {
///     let (right_sender, left_receiver) = right;
///     assert_eq!(left_receiver.recv(), Ok(1));
///     right_sender.send(5).unwrap();
/// });
///
/// // The `Left` process
/// let (left_sender, right_receiver) = left;
/// left_sender.send(1).unwrap();
/// assert_eq!(right_receiver.recv(), Ok(5));
/// ```
pub fn left_bounded_bridge<L, R>(cap: usize) -> (LeftChannelSplit<L, R>, RightChannelSplit<L, R>) {
    let (left_tx, right_rx) = bounded::<L>(cap);
    let (right_tx, left_rx) = unbounded::<R>();

    ((left_tx, left_rx), (right_tx, right_rx))
}

/// Creates two pairs of channels for the `Left` and `Right` types with the `Right` side bounded.
///
/// The [`Sender<R>`] and [`Receiver<R>`] are bound by the `cap` and the [`Sender<L>`] and
/// [`Receiver<L>`] is unbound
///
/// This returns a [`LeftChannelSplit<L, R>`] and [`RightChannelSplit<L, R>`] which hold a sender
/// and a receiver of the opposite type
///
/// # Examples
///
/// ```rust
/// use std::thread;
/// use suplex::right_bounded_bridge;
///
/// let (left, right) = right_bounded_bridge::<usize, usize>(1);
///
/// // The `Right` process
/// thread::spawn(move || {
///     let (right_sender, left_receiver) = right;
///     assert_eq!(left_receiver.recv(), Ok(1));
///     right_sender.send(5).unwrap();
/// });
///
/// // The `Left` process
/// let (left_sender, right_receiver) = left;
/// left_sender.send(1).unwrap();
/// assert_eq!(right_receiver.recv(), Ok(5));
/// ```
pub fn right_bounded_bridge<L, R>(cap: usize) -> (LeftChannelSplit<L, R>, RightChannelSplit<L, R>) {
    let (left_tx, right_rx) = unbounded::<L>();
    let (right_tx, left_rx) = bounded::<R>(cap);

    ((left_tx, left_rx), (right_tx, right_rx))
}

#[cfg(test)]
mod bridge_tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn unbounded_send_recv_left_single_thread() {
        let b: Bridge<String, usize> = Bridge::unbounded();
        for i in 0..5 {
            let r = b.send_to_right(format!("iteration: {i}"));
            assert!(r.is_ok());
        }

        assert_eq!(b.left_receiver_len(), 5);

        for i in 0..5 {
            let r = b.recv_from_left();
            assert!(r.is_ok());
            let value = r.unwrap();
            assert_eq!(value, format!("iteration: {i}"));
        }
    }

    #[test]
    fn unbounded_send_recv_right_single_thread() {
        let b: Bridge<String, usize> = Bridge::unbounded();
        for i in 0..5 {
            let r = b.send_to_left(i);
            assert!(r.is_ok());
        }

        assert_eq!(b.right_receiver_len(), 5);

        for i in 0..5 {
            let r = b.recv_from_right();
            assert!(r.is_ok());
            let value = r.unwrap();
            assert_eq!(value, i);
        }
    }

    #[test]
    fn bounded() {
        let b: Bridge<String, usize> = Bridge::bounded(10);
        assert_eq!(b.left_sender_capacity(), Some(10));
        assert_eq!(b.right_sender_capacity(), Some(10));
        assert_eq!(b.left_receiver_capacity(), Some(10));
        assert_eq!(b.right_receiver_capacity(), Some(10));
    }

    #[test]
    fn left_bounded() {
        let b: Bridge<String, usize> = Bridge::left_bounded(10);
        assert_eq!(b.left_sender_capacity(), Some(10));
        assert_eq!(b.right_sender_capacity(), None);
        assert_eq!(b.left_receiver_capacity(), Some(10));
        assert_eq!(b.right_receiver_capacity(), None);
    }

    #[test]
    fn right_bounded() {
        let b: Bridge<String, usize> = Bridge::right_bounded(10);
        assert_eq!(b.left_sender_capacity(), None);
        assert_eq!(b.right_sender_capacity(), Some(10));
        assert_eq!(b.left_receiver_capacity(), None);
        assert_eq!(b.right_receiver_capacity(), Some(10));
    }

    #[test]
    fn unbounded_left_send_recv_multi_thread() {
        let b: Bridge<String, usize> = Bridge::unbounded();
        let right_b = b.clone();

        let jh = thread::spawn(move || {
            for i in 0..5 {
                let r = right_b.recv_from_left();
                assert!(r.is_ok());
                let value = r.unwrap();
                assert_eq!(value, format!("iteration: {i}"));
            }
        });

        for i in 0..5 {
            let r = b.send_to_right(format!("iteration: {i}"));
            assert!(r.is_ok());
        }

        jh.join().unwrap();
    }

    #[test]
    fn unbounded_right_send_recv_multi_thread() {
        let b: Bridge<String, usize> = Bridge::unbounded();
        let right_b = b.clone();

        let jh = thread::spawn(move || {
            for i in 0..5 {
                let r = right_b.recv_from_right();
                assert!(r.is_ok());
                let value = r.unwrap();
                assert_eq!(value, i);
            }
        });

        for i in 0..5 {
            let r = b.send_to_left(i);
            assert!(r.is_ok());
        }

        jh.join().unwrap();
    }

    #[test]
    fn unbounded_left_send_recv_multi_thread_arc() {
        let b: Arc<Bridge<String, usize>> = Arc::new(Bridge::unbounded());
        let right_b = Arc::clone(&b);
        let jh = thread::spawn(move || {
            for i in 0..5 {
                let r = right_b.recv_from_left();
                assert!(r.is_ok());
                let value = r.unwrap();
                assert_eq!(value, format!("iteration: {i}"));
            }
        });

        for i in 0..5 {
            let r = b.send_to_right(format!("iteration: {i}"));
            assert!(r.is_ok());
        }

        jh.join().unwrap();
    }

    #[test]
    fn unbounded_right_send_recv_multi_thread_arc() {
        let b: Arc<Bridge<String, usize>> = Arc::new(Bridge::unbounded());
        let right_b = Arc::clone(&b);
        let jh = thread::spawn(move || {
            for i in 0..5 {
                let r = right_b.recv_from_right();
                assert!(r.is_ok());
                let value = r.unwrap();
                assert_eq!(value, i);
            }
        });

        for i in 0..5 {
            let r = b.send_to_left(i);
            assert!(r.is_ok());
        }

        jh.join().unwrap();
    }
}
