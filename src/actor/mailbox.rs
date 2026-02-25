use crossbeam::queue::ArrayQueue;
use std::sync::Arc;

use super::message::Message;

/// Default capacity for mailboxes.
pub const DEFAULT_MAILBOX_CAPACITY: usize = 256;

/// A lock-free mailbox for actor message passing.
///
/// Uses a bounded lock-free queue (ArrayQueue) for both inbox and outbox.
/// This ensures that message passing never blocks or requires locks,
/// which is critical for real-time audio applications.
pub struct Mailbox {
    /// Incoming messages for this actor.
    inbox: Arc<ArrayQueue<Message>>,
    /// Outgoing messages from this actor.
    outbox: Arc<ArrayQueue<Message>>,
    /// Capacity of the queues.
    capacity: usize,
}

impl Mailbox {
    /// Create a new mailbox with the default capacity.
    pub fn new() -> Self {
        Self::with_capacity(DEFAULT_MAILBOX_CAPACITY)
    }

    /// Create a new mailbox with a specific capacity.
    ///
    /// Capacity is clamped to a minimum of 1 to ensure the mailbox can receive messages.
    pub fn with_capacity(capacity: usize) -> Self {
        let capacity = capacity.max(1); // Ensure at least capacity of 1
        Self {
            inbox: Arc::new(ArrayQueue::new(capacity)),
            outbox: Arc::new(ArrayQueue::new(capacity)),
            capacity,
        }
    }

    /// Try to push a message to the inbox.
    ///
    /// Returns `Err(msg)` if the inbox is full.
    pub fn push_inbox(&self, msg: Message) -> Result<(), Message> {
        self.inbox.push(msg)
    }

    /// Try to pop a message from the inbox.
    pub fn pop_inbox(&self) -> Option<Message> {
        self.inbox.pop()
    }

    /// Try to push a message to the outbox.
    ///
    /// Returns `Err(msg)` if the outbox is full.
    pub fn push_outbox(&self, msg: Message) -> Result<(), Message> {
        self.outbox.push(msg)
    }

    /// Try to pop a message from the outbox.
    pub fn pop_outbox(&self) -> Option<Message> {
        self.outbox.pop()
    }

    /// Check if the inbox is empty.
    pub fn inbox_is_empty(&self) -> bool {
        self.inbox.is_empty()
    }

    /// Check if the outbox is empty.
    pub fn outbox_is_empty(&self) -> bool {
        self.outbox.is_empty()
    }

    /// Get the number of messages in the inbox.
    pub fn inbox_len(&self) -> usize {
        self.inbox.len()
    }

    /// Get the number of messages in the outbox.
    pub fn outbox_len(&self) -> usize {
        self.outbox.len()
    }

    /// Get the capacity of the mailbox.
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Get a handle to the inbox for external producers.
    pub fn inbox_handle(&self) -> InboxHandle {
        InboxHandle {
            queue: Arc::clone(&self.inbox),
        }
    }

    /// Get a handle to the outbox for external consumers.
    pub fn outbox_handle(&self) -> OutboxHandle {
        OutboxHandle {
            queue: Arc::clone(&self.outbox),
        }
    }

    /// Drain all messages from the inbox into a vector.
    pub fn drain_inbox(&self) -> Vec<Message> {
        let mut messages = Vec::with_capacity(self.inbox.len());
        while let Some(msg) = self.inbox.pop() {
            messages.push(msg);
        }
        messages
    }

    /// Drain all messages from the outbox into a vector.
    pub fn drain_outbox(&self) -> Vec<Message> {
        let mut messages = Vec::with_capacity(self.outbox.len());
        while let Some(msg) = self.outbox.pop() {
            messages.push(msg);
        }
        messages
    }
}

impl Default for Mailbox {
    fn default() -> Self {
        Self::new()
    }
}

/// Handle for pushing messages to an actor's inbox.
///
/// This can be shared across threads to allow external producers
/// to send messages to an actor.
#[derive(Clone)]
pub struct InboxHandle {
    queue: Arc<ArrayQueue<Message>>,
}

impl InboxHandle {
    /// Try to push a message to the inbox.
    pub fn push(&self, msg: Message) -> Result<(), Message> {
        self.queue.push(msg)
    }

    /// Check if the inbox is full.
    pub fn is_full(&self) -> bool {
        self.queue.is_full()
    }

    /// Get the current length of the inbox.
    pub fn len(&self) -> usize {
        self.queue.len()
    }

    /// Check if the inbox is empty.
    pub fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }
}

/// Handle for consuming messages from an actor's outbox.
///
/// This can be shared across threads to allow external consumers
/// to receive messages from an actor.
#[derive(Clone)]
pub struct OutboxHandle {
    queue: Arc<ArrayQueue<Message>>,
}

impl OutboxHandle {
    /// Try to pop a message from the outbox.
    pub fn pop(&self) -> Option<Message> {
        self.queue.pop()
    }

    /// Check if the outbox is empty.
    pub fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }

    /// Get the current length of the outbox.
    pub fn len(&self) -> usize {
        self.queue.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use slotmap::SlotMap;
    use std::sync::Arc;
    use std::thread;

    use crate::actor::ActorId;

    struct DummyPayload(i32);

    fn make_test_message(value: i32) -> Message {
        let mut actors: SlotMap<ActorId, ()> = SlotMap::with_key();
        let source = actors.insert(());
        let target = actors.insert(());
        Message::new(target, DummyPayload(value), source)
    }

    #[test]
    fn mailbox_basic_operations() {
        let mailbox = Mailbox::new();

        assert!(mailbox.inbox_is_empty());
        assert!(mailbox.outbox_is_empty());
    }

    #[test]
    fn mailbox_push_pop() {
        let mailbox = Mailbox::new();
        let msg = make_test_message(42);

        assert!(mailbox.push_inbox(msg).is_ok());
        assert!(!mailbox.inbox_is_empty());
        assert_eq!(mailbox.inbox_len(), 1);

        let popped = mailbox.pop_inbox();
        assert!(popped.is_some());
        assert!(mailbox.inbox_is_empty());
    }

    #[test]
    fn mailbox_capacity() {
        let mailbox = Mailbox::with_capacity(2);
        assert_eq!(mailbox.capacity(), 2);

        // Fill the mailbox
        assert!(mailbox.push_inbox(make_test_message(1)).is_ok());
        assert!(mailbox.push_inbox(make_test_message(2)).is_ok());

        // Third push should fail (backpressure)
        let result = mailbox.push_inbox(make_test_message(3));
        assert!(result.is_err());
    }

    #[test]
    fn inbox_handle_sharing() {
        let mailbox = Mailbox::new();
        let handle1 = mailbox.inbox_handle();
        let handle2 = mailbox.inbox_handle();

        // Both handles should work
        assert!(handle1.is_empty());
        assert!(handle2.is_empty());

        // Push via handle1
        handle1.push(make_test_message(1)).unwrap();

        // Both handles see the message
        assert!(!handle1.is_empty());
        assert!(!handle2.is_empty());
        assert_eq!(handle1.len(), 1);
        assert_eq!(handle2.len(), 1);
    }

    #[test]
    fn concurrent_push_single_mailbox() {
        let mailbox = Arc::new(Mailbox::with_capacity(1000));
        let handle = mailbox.inbox_handle();

        let mut handles = Vec::new();
        const NUM_THREADS: usize = 4;
        const MESSAGES_PER_THREAD: usize = 100;

        for _ in 0..NUM_THREADS {
            let h = handle.clone();
            handles.push(thread::spawn(move || {
                let mut success_count = 0;
                for i in 0..MESSAGES_PER_THREAD {
                    let msg = make_test_message(i as i32);
                    if h.push(msg).is_ok() {
                        success_count += 1;
                    }
                }
                success_count
            }));
        }

        let total_pushed: usize = handles.into_iter().map(|h| h.join().unwrap()).sum();

        // All messages should have been pushed (mailbox has capacity 1000)
        assert_eq!(total_pushed, NUM_THREADS * MESSAGES_PER_THREAD);
        assert_eq!(mailbox.inbox_len(), NUM_THREADS * MESSAGES_PER_THREAD);
    }

    #[test]
    fn concurrent_push_pop() {
        use std::time::Duration;

        let mailbox = Arc::new(Mailbox::with_capacity(100));
        let handle = mailbox.inbox_handle();

        const NUM_MESSAGES: usize = 1000;

        // Producer thread
        let producer_handle = handle.clone();
        let producer = thread::spawn(move || {
            let mut success = 0;
            let mut backoff = 1;
            for i in 0..NUM_MESSAGES {
                let msg = make_test_message(i as i32);
                // Retry with exponential backoff if full
                loop {
                    match producer_handle.push(msg.clone()) {
                        Ok(()) => {
                            success += 1;
                            backoff = 1; // Reset backoff on success
                            break;
                        }
                        Err(_) => {
                            // Exponential backoff with cap
                            thread::sleep(Duration::from_micros(backoff));
                            backoff = (backoff * 2).min(1000);
                        }
                    }
                }
            }
            success
        });

        // Consumer thread
        let consumer_mailbox = Arc::clone(&mailbox);
        let consumer = thread::spawn(move || {
            let mut consumed = 0;
            let mut backoff = 1;
            while consumed < NUM_MESSAGES {
                if consumer_mailbox.pop_inbox().is_some() {
                    consumed += 1;
                    backoff = 1; // Reset backoff on success
                } else {
                    // Exponential backoff with cap
                    thread::sleep(Duration::from_micros(backoff));
                    backoff = (backoff * 2).min(1000);
                }
            }
            consumed
        });

        let produced = producer.join().unwrap();
        let consumed = consumer.join().unwrap();

        assert_eq!(produced, NUM_MESSAGES);
        assert_eq!(consumed, NUM_MESSAGES);
        assert!(mailbox.inbox_is_empty());
    }

    #[test]
    fn drain_returns_all_messages() {
        let mailbox = Mailbox::new();

        for i in 0..10 {
            mailbox.push_inbox(make_test_message(i)).unwrap();
        }

        let drained = mailbox.drain_inbox();
        assert_eq!(drained.len(), 10);
        assert!(mailbox.inbox_is_empty());
    }
}
