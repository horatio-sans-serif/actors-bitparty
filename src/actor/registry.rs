use std::any::TypeId;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::RwLock;
use slotmap::SlotMap;
use smallvec::SmallVec;

use super::actor::{Actor, ActorError, ActorId, HandleResult, SupervisionStrategy};
use super::context::ActorContext;
use super::mailbox::{InboxHandle, Mailbox};
use super::message::Message;

/// Metrics for tracking actor system health.
///
/// Note: Counters use wrapping addition and will wrap around at `u64::MAX`.
/// For long-running applications, consider periodic snapshotting if overflow
/// could be an issue (though 2^64 operations would take millennia at typical rates).
#[derive(Debug, Default)]
pub struct RegistryMetrics {
    /// Total messages delivered successfully (wraps at u64::MAX).
    pub messages_delivered: AtomicU64,
    /// Total messages dropped due to full mailboxes (wraps at u64::MAX).
    pub messages_dropped: AtomicU64,
    /// Total broadcast messages sent (wraps at u64::MAX).
    pub broadcasts_sent: AtomicU64,
    /// Total actors spawned (wraps at u64::MAX).
    pub actors_spawned: AtomicU64,
    /// Total actors despawned (wraps at u64::MAX).
    pub actors_despawned: AtomicU64,
    /// Total errors during message handling (wraps at u64::MAX).
    pub handle_errors: AtomicU64,
}

impl RegistryMetrics {
    /// Get a snapshot of the current metrics.
    ///
    /// Uses `Ordering::Acquire` to ensure all preceding writes are visible,
    /// providing a consistent view of the metrics at the time of the snapshot.
    pub fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            messages_delivered: self.messages_delivered.load(Ordering::Acquire),
            messages_dropped: self.messages_dropped.load(Ordering::Acquire),
            broadcasts_sent: self.broadcasts_sent.load(Ordering::Acquire),
            actors_spawned: self.actors_spawned.load(Ordering::Acquire),
            actors_despawned: self.actors_despawned.load(Ordering::Acquire),
            handle_errors: self.handle_errors.load(Ordering::Acquire),
        }
    }
}

/// A point-in-time snapshot of registry metrics.
#[derive(Debug, Clone, Default)]
pub struct MetricsSnapshot {
    pub messages_delivered: u64,
    pub messages_dropped: u64,
    pub broadcasts_sent: u64,
    pub actors_spawned: u64,
    pub actors_despawned: u64,
    pub handle_errors: u64,
}

/// Result of sending a message.
#[must_use = "send result should be checked for delivery failure or backpressure"]
#[derive(Debug)]
pub enum SendResult {
    /// Message was delivered successfully.
    Delivered,
    /// Mailbox is full - message returned for retry or handling.
    MailboxFull(Message),
    /// Target actor was not found - message returned.
    ActorNotFound(Message),
}

impl SendResult {
    /// Check if the send was successful.
    pub fn is_ok(&self) -> bool {
        matches!(self, SendResult::Delivered)
    }

    /// Check if the send failed.
    pub fn is_err(&self) -> bool {
        !self.is_ok()
    }
}

/// Entry for an actor in the registry.
struct ActorEntry {
    actor: Box<dyn Actor>,
    mailbox: Mailbox,
    parent: Option<ActorId>,
    children: SmallVec<[ActorId; 4]>,
    /// Flag indicating this actor has pending messages.
    has_pending: bool,
}

/// Central registry managing all actors in the system.
///
/// The registry is responsible for:
/// - Storing and managing actor lifecycles
/// - Routing messages between actors
/// - Maintaining parent/child relationships
/// - Processing the actor event loop
/// - Tracking metrics for system health monitoring
pub struct ActorRegistry {
    /// All registered actors.
    actors: SlotMap<ActorId, ActorEntry>,
    /// Mapping from message type to actors that handle it.
    handlers: HashMap<TypeId, SmallVec<[ActorId; 8]>>,
    /// Inbox handles for external message delivery.
    inbox_handles: HashMap<ActorId, InboxHandle>,
    /// The root actor (if any).
    root: Option<ActorId>,
    /// Set of actor IDs with pending messages (for optimized tick).
    pending_actors: SmallVec<[ActorId; 16]>,
    /// Metrics for monitoring.
    metrics: RegistryMetrics,
}

impl ActorRegistry {
    /// Maximum distinct message types the registry can track.
    pub const MAX_MESSAGE_TYPES: usize = 4096;

    /// Create a new empty registry.
    pub fn new() -> Self {
        Self {
            actors: SlotMap::with_key(),
            handlers: HashMap::new(),
            inbox_handles: HashMap::new(),
            root: None,
            pending_actors: SmallVec::new(),
            metrics: RegistryMetrics::default(),
        }
    }

    /// Get the registry metrics.
    pub fn metrics(&self) -> &RegistryMetrics {
        &self.metrics
    }

    /// Spawn a new actor into the registry.
    pub fn spawn(&mut self, mut actor: Box<dyn Actor>) -> ActorId {
        let mailbox = Mailbox::new();
        let inbox_handle = mailbox.inbox_handle();

        let id = self.actors.insert_with_key(|id| {
            actor.set_id(id);
            ActorEntry {
                actor,
                mailbox,
                parent: None,
                children: SmallVec::new(),
                has_pending: false,
            }
        });

        // Register handlers (check for duplicates and capacity)
        if let Some(entry) = self.actors.get(id) {
            for &type_id in entry.actor.handles() {
                // Check capacity before adding new message type
                if !self.handlers.contains_key(&type_id)
                    && self.handlers.len() >= Self::MAX_MESSAGE_TYPES
                {
                    log::warn!(
                        "Maximum message types ({}) reached, cannot register handler for new type",
                        Self::MAX_MESSAGE_TYPES
                    );
                    continue;
                }
                let handlers = self.handlers.entry(type_id).or_default();
                if !handlers.contains(&id) {
                    handlers.push(id);
                }
            }
        }

        // Store inbox handle
        self.inbox_handles.insert(id, inbox_handle);

        // Call on_spawn
        let mut ctx = ActorContext::new(id);
        if let Some(entry) = self.actors.get_mut(id) {
            entry.actor.on_spawn(&mut ctx);
        }

        // Process any spawn-time requests
        self.process_context_requests(ctx);

        // Set as root if first actor
        if self.root.is_none() {
            self.root = Some(id);
        }

        self.metrics.actors_spawned.fetch_add(1, Ordering::Relaxed);
        id
    }

    /// Spawn an actor with a parent.
    pub fn spawn_with_parent(&mut self, actor: Box<dyn Actor>, parent: ActorId) -> ActorId {
        let id = self.spawn(actor);

        // Set parent relationship
        if let Some(entry) = self.actors.get_mut(id) {
            entry.parent = Some(parent);
            entry.actor.set_parent(Some(parent));
        }

        // Add to parent's children
        if let Some(parent_entry) = self.actors.get_mut(parent) {
            parent_entry.children.push(id);
        }

        id
    }

    /// Despawn an actor and all its children.
    ///
    /// Uses an iterative approach to avoid stack overflow with deeply nested hierarchies.
    pub fn despawn(&mut self, id: ActorId) {
        // Collect all actors to despawn (children first, then parents)
        let mut to_despawn = Vec::new();
        let mut stack = vec![id];

        // Build the full list of actors to despawn (post-order traversal)
        while let Some(current) = stack.pop() {
            to_despawn.push(current);
            if let Some(entry) = self.actors.get(current) {
                // Push children onto stack (they'll be processed first)
                for &child in &entry.children {
                    stack.push(child);
                }
            }
        }

        // Process in reverse order (children before parents)
        for actor_id in to_despawn.into_iter().rev() {
            self.despawn_single(actor_id);
        }
    }

    /// Despawn a single actor without processing children.
    fn despawn_single(&mut self, id: ActorId) {
        // Call on_despawn
        let mut ctx = ActorContext::new(id);
        if let Some(entry) = self.actors.get_mut(id) {
            entry.actor.on_despawn(&mut ctx);
        }

        // Remove from handlers
        if let Some(entry) = self.actors.get(id) {
            for &type_id in entry.actor.handles() {
                if let Some(handlers) = self.handlers.get_mut(&type_id) {
                    handlers.retain(|h| *h != id);
                }
            }
        }

        // Clean up empty handler lists to prevent memory growth
        self.handlers.retain(|_, v| !v.is_empty());

        // Remove from parent's children
        if let Some(entry) = self.actors.get(id) {
            if let Some(parent) = entry.parent {
                if let Some(parent_entry) = self.actors.get_mut(parent) {
                    parent_entry.children.retain(|c| *c != id);
                }
            }
        }

        // Remove from pending actors list
        self.pending_actors.retain(|a| *a != id);

        // Remove inbox handle
        self.inbox_handles.remove(&id);

        // Remove actor
        self.actors.remove(id);

        // Clear root if this was it
        if self.root == Some(id) {
            self.root = None;
        }

        self.metrics.actors_despawned.fetch_add(1, Ordering::Relaxed);
    }

    /// Send a message to a specific actor.
    ///
    /// Returns `SendResult::Delivered` if the message was queued successfully,
    /// `SendResult::MailboxFull` if the mailbox is full (backpressure),
    /// or `SendResult::ActorNotFound` if the target actor doesn't exist.
    pub fn send(&mut self, msg: Message) -> SendResult {
        if let Some(target) = msg.target {
            if let Some(handle) = self.inbox_handles.get(&target) {
                match handle.push(msg) {
                    Ok(()) => {
                        // Mark actor as having pending messages
                        if let Some(entry) = self.actors.get_mut(target) {
                            if !entry.has_pending {
                                entry.has_pending = true;
                                self.pending_actors.push(target);
                            }
                        }
                        self.metrics.messages_delivered.fetch_add(1, Ordering::Relaxed);
                        return SendResult::Delivered;
                    }
                    Err(msg) => {
                        self.metrics.messages_dropped.fetch_add(1, Ordering::Relaxed);
                        log::warn!(
                            "Message dropped: mailbox full for actor {:?}, type: {}",
                            target,
                            msg.payload_type_name()
                        );
                        return SendResult::MailboxFull(msg);
                    }
                }
            } else {
                return SendResult::ActorNotFound(msg);
            }
        }
        self.metrics.messages_dropped.fetch_add(1, Ordering::Relaxed);
        SendResult::ActorNotFound(msg)
    }

    /// Broadcast a message to all actors that handle its type.
    ///
    /// Messages are cloned (using Arc-based sharing) for each recipient.
    /// Returns the number of actors that received the message.
    #[must_use = "broadcast returns the number of recipients, check for delivery failures"]
    pub fn broadcast(&mut self, msg: Message) -> usize {
        // First, collect handler IDs without borrowing self mutably
        let handler_ids: SmallVec<[ActorId; 8]> = self
            .handlers
            .get(&msg.payload_type)
            .map(|h| h.iter().copied().filter(|&id| id != msg.source).collect())
            .unwrap_or_default();

        if handler_ids.is_empty() {
            return 0;
        }

        let mut delivered = 0;

        // Now send to each handler
        for handler_id in handler_ids {
            // Clone the message (Arc-based, efficient)
            let cloned_msg = msg.clone().retarget(handler_id);
            if self.send(cloned_msg).is_ok() {
                delivered += 1;
            }
        }

        if delivered > 0 {
            self.metrics.broadcasts_sent.fetch_add(1, Ordering::Relaxed);
        }
        delivered
    }

    /// Process one tick of the actor system.
    ///
    /// This drains all mailboxes, delivers messages, and processes
    /// any spawn/despawn requests that result from handling.
    ///
    /// Optimized to only process actors with pending messages.
    pub fn tick(&mut self) {
        // Swap out pending actors list to avoid borrow issues
        let pending = std::mem::take(&mut self.pending_actors);

        // Collect messages only from actors with pending messages
        // Use a temporary Vec to avoid borrow issues
        let mut pending_messages: Vec<(ActorId, Message)> = Vec::new();

        for &id in &pending {
            if let Some(entry) = self.actors.get_mut(id) {
                entry.has_pending = false;
                while let Some(msg) = entry.mailbox.pop_inbox() {
                    pending_messages.push((id, msg));
                }
            }
        }

        // Collect results that need post-processing
        let mut errors_to_handle: Vec<(ActorId, ActorError)> = Vec::new();
        let mut contexts_to_process: Vec<ActorContext> = Vec::new();

        // Process each message
        for (target_id, msg) in pending_messages {
            let mut ctx = ActorContext::new(target_id);

            let result = if let Some(entry) = self.actors.get_mut(target_id) {
                entry.actor.handle(msg, &mut ctx)
            } else {
                continue;
            };

            // Collect errors for later handling
            if let HandleResult::Error(ref err) = result {
                self.metrics.handle_errors.fetch_add(1, Ordering::Relaxed);
                log::error!("Actor {:?} handle error: {}", target_id, err);
                errors_to_handle.push((target_id, err.clone()));
            }

            contexts_to_process.push(ctx);
        }

        // Process errors with supervision (after message processing loop)
        for (actor_id, error) in errors_to_handle {
            self.handle_actor_error(actor_id, error);
        }

        // Process context requests (after message processing loop)
        for ctx in contexts_to_process {
            self.process_context_requests(ctx);
        }

        // Route outgoing messages from all actors
        let mut outgoing: Vec<Message> = Vec::new();
        for (_, entry) in self.actors.iter() {
            while let Some(msg) = entry.mailbox.pop_outbox() {
                outgoing.push(msg);
            }
        }

        for msg in outgoing {
            if msg.target.is_some() {
                let _ = self.send(msg);
            } else {
                let _ = self.broadcast(msg);
            }
        }
    }

    /// Handle an actor error by invoking supervision.
    ///
    /// Uses an iterative approach with cycle detection to prevent infinite loops
    /// in case of cyclic parent relationships.
    fn handle_actor_error(&mut self, actor_id: ActorId, error: ActorError) {
        use std::collections::HashSet;

        let mut current_actor = actor_id;
        let mut current_error = error;
        let mut visited: HashSet<ActorId> = HashSet::new();
        let mut contexts_to_process: Vec<ActorContext> = Vec::new();

        loop {
            // Cycle detection
            if !visited.insert(current_actor) {
                log::error!(
                    "Cyclic supervision detected at {:?}, stopping escalation",
                    current_actor
                );
                break;
            }

            // Get the parent to supervise
            let parent_id = self.actors.get(current_actor).and_then(|e| e.parent);

            let Some(parent_id) = parent_id else {
                // No parent - log at root level
                log::error!(
                    "Unhandled actor error at root {:?}: {}",
                    current_actor,
                    current_error
                );
                break;
            };

            let mut ctx = ActorContext::new(parent_id);
            let strategy = if let Some(parent_entry) = self.actors.get_mut(parent_id) {
                parent_entry
                    .actor
                    .on_child_error(current_actor, &current_error, &mut ctx)
            } else {
                SupervisionStrategy::Escalate
            };

            contexts_to_process.push(ctx);

            match strategy {
                SupervisionStrategy::Resume => {
                    log::debug!(
                        "Supervisor {:?} resuming actor {:?}",
                        parent_id,
                        current_actor
                    );
                    break;
                }
                SupervisionStrategy::Restart => {
                    log::info!(
                        "Supervisor {:?} restarting actor {:?}",
                        parent_id,
                        current_actor
                    );
                    // Call on_despawn, reset state, then on_spawn.
                    let mut despawn_ctx = ActorContext::new(current_actor);
                    if let Some(entry) = self.actors.get_mut(current_actor) {
                        entry.actor.on_despawn(&mut despawn_ctx);
                        entry.actor.reset();
                        let mut spawn_ctx = ActorContext::new(current_actor);
                        entry.actor.on_spawn(&mut spawn_ctx);
                        // Drain any stale messages from the mailbox.
                        while entry.mailbox.pop_inbox().is_some() {}
                        contexts_to_process.push(spawn_ctx);
                    }
                    contexts_to_process.push(despawn_ctx);
                    break;
                }
                SupervisionStrategy::Stop => {
                    log::info!(
                        "Supervisor {:?} stopping actor {:?}",
                        parent_id,
                        current_actor
                    );
                    self.despawn(current_actor);
                    break;
                }
                SupervisionStrategy::Escalate => {
                    log::debug!(
                        "Escalating error from {:?} to {:?}",
                        current_actor,
                        parent_id
                    );
                    current_error = ActorError::ChildError {
                        actor: current_actor,
                        error: Box::new(current_error),
                    };
                    current_actor = parent_id;
                    // Continue loop for escalation
                }
            }
        }

        // Process all collected contexts
        for ctx in contexts_to_process {
            self.process_context_requests(ctx);
        }
    }

    /// Process spawn/despawn requests from a context.
    fn process_context_requests(&mut self, mut ctx: ActorContext) {
        // Handle outgoing messages
        for msg in ctx.take_outgoing() {
            if msg.target.is_some() {
                let _ = self.send(msg);
            } else {
                let _ = self.broadcast(msg);
            }
        }

        // Handle spawn requests
        for request in ctx.take_spawn_requests() {
            let actor = (request.actor)();
            if let Some(parent) = request.parent {
                self.spawn_with_parent(actor, parent);
            } else {
                self.spawn(actor);
            }
        }

        // Handle despawn requests
        for id in ctx.take_despawn_requests() {
            self.despawn(id);
        }

        // Register inbox handles
        for (id, handle) in ctx.take_inbox_handles() {
            self.inbox_handles.insert(id, handle);
        }

        // Register handlers (with duplicate check)
        for (id, type_id) in ctx.take_handlers() {
            let handlers = self.handlers.entry(type_id).or_default();
            if !handlers.contains(&id) {
                handlers.push(id);
            }
        }
    }

    /// Get an actor by ID.
    pub fn get(&self, id: ActorId) -> Option<&dyn Actor> {
        self.actors.get(id).map(|e| e.actor.as_ref())
    }

    /// Get a mutable reference to an actor by ID.
    pub fn get_mut(&mut self, id: ActorId) -> Option<&mut Box<dyn Actor>> {
        self.actors.get_mut(id).map(|e| &mut e.actor)
    }

    /// Get the root actor.
    pub fn root(&self) -> Option<ActorId> {
        self.root
    }

    /// Get the number of actors in the registry.
    pub fn len(&self) -> usize {
        self.actors.len()
    }

    /// Check if the registry is empty.
    pub fn is_empty(&self) -> bool {
        self.actors.is_empty()
    }

    /// Get an inbox handle for an actor.
    pub fn inbox(&self, id: ActorId) -> Option<InboxHandle> {
        self.inbox_handles.get(&id).cloned()
    }

    /// Get the parent of an actor.
    pub fn parent(&self, id: ActorId) -> Option<ActorId> {
        self.actors.get(id).and_then(|e| e.parent)
    }

    /// Get the children of an actor.
    pub fn children(&self, id: ActorId) -> &[ActorId] {
        self.actors
            .get(id)
            .map(|e| e.children.as_slice())
            .unwrap_or(&[])
    }

    /// Iterate over all actor IDs.
    pub fn iter(&self) -> impl Iterator<Item = ActorId> + '_ {
        self.actors.keys()
    }

    /// Find actors that handle a specific message type.
    pub fn handlers_for(&self, type_id: TypeId) -> &[ActorId] {
        self.handlers
            .get(&type_id)
            .map(|v| v.as_slice())
            .unwrap_or(&[])
    }
}

impl Default for ActorRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Thread-safe wrapper around ActorRegistry.
pub struct SharedRegistry {
    inner: RwLock<ActorRegistry>,
}

impl SharedRegistry {
    pub fn new() -> Self {
        Self {
            inner: RwLock::new(ActorRegistry::new()),
        }
    }

    pub fn read(&self) -> parking_lot::RwLockReadGuard<'_, ActorRegistry> {
        self.inner.read()
    }

    pub fn write(&self) -> parking_lot::RwLockWriteGuard<'_, ActorRegistry> {
        self.inner.write()
    }
}

impl Default for SharedRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct TestActor {
        id: ActorId,
        value: i32,
    }

    impl TestActor {
        fn new(value: i32) -> Self {
            Self {
                id: ActorId::default(),
                value,
            }
        }
    }

    impl Actor for TestActor {
        fn id(&self) -> ActorId {
            self.id
        }

        fn set_id(&mut self, id: ActorId) {
            self.id = id;
        }

        fn handles(&self) -> &[TypeId] {
            &[]
        }

        fn handle(&mut self, _msg: Message, _ctx: &mut ActorContext) -> HandleResult {
            HandleResult::NotHandled
        }
    }

    #[test]
    fn spawn_and_despawn() {
        let mut registry = ActorRegistry::new();

        let id = registry.spawn(Box::new(TestActor::new(42)));
        assert_eq!(registry.len(), 1);
        assert!(registry.get(id).is_some());

        registry.despawn(id);
        assert_eq!(registry.len(), 0);
        assert!(registry.get(id).is_none());
    }

    #[test]
    fn parent_child_relationship() {
        let mut registry = ActorRegistry::new();

        let parent_id = registry.spawn(Box::new(TestActor::new(1)));
        let child_id = registry.spawn_with_parent(Box::new(TestActor::new(2)), parent_id);

        assert_eq!(registry.parent(child_id), Some(parent_id));
        assert_eq!(registry.children(parent_id), &[child_id]);

        // Despawning parent should also despawn children
        registry.despawn(parent_id);
        assert!(registry.get(parent_id).is_none());
        assert!(registry.get(child_id).is_none());
    }

    #[test]
    fn root_actor() {
        let mut registry = ActorRegistry::new();

        assert!(registry.root().is_none());

        let id = registry.spawn(Box::new(TestActor::new(1)));
        assert_eq!(registry.root(), Some(id));

        registry.despawn(id);
        assert!(registry.root().is_none());
    }

    #[test]
    fn metrics_tracking() {
        let mut registry = ActorRegistry::new();

        let id = registry.spawn(Box::new(TestActor::new(1)));
        let metrics = registry.metrics().snapshot();
        assert_eq!(metrics.actors_spawned, 1);

        registry.despawn(id);
        let metrics = registry.metrics().snapshot();
        assert_eq!(metrics.actors_despawned, 1);
    }

    #[test]
    fn broadcast_delivers_to_handlers() {
        use std::any::TypeId;

        struct BroadcastHandler {
            id: ActorId,
            handles: Vec<TypeId>,
            received: std::sync::atomic::AtomicBool,
        }

        struct TestMessage(i32);

        impl BroadcastHandler {
            fn new() -> Self {
                Self {
                    id: ActorId::default(),
                    handles: vec![TypeId::of::<TestMessage>()],
                    received: std::sync::atomic::AtomicBool::new(false),
                }
            }
        }

        impl Actor for BroadcastHandler {
            fn id(&self) -> ActorId {
                self.id
            }

            fn set_id(&mut self, id: ActorId) {
                self.id = id;
            }

            fn handles(&self) -> &[TypeId] {
                &self.handles
            }

            fn handle(&mut self, msg: Message, _ctx: &mut ActorContext) -> HandleResult {
                if msg.is::<TestMessage>() {
                    self.received.store(true, std::sync::atomic::Ordering::SeqCst);
                    HandleResult::Handled
                } else {
                    HandleResult::NotHandled
                }
            }
        }

        let mut registry = ActorRegistry::new();

        // Spawn a sender
        let sender_id = registry.spawn(Box::new(TestActor::new(0)));

        // Spawn handlers
        let _handler1_id = registry.spawn(Box::new(BroadcastHandler::new()));
        let _handler2_id = registry.spawn(Box::new(BroadcastHandler::new()));

        // Broadcast a message
        let msg = Message::broadcast(TestMessage(42), sender_id);
        let delivered = registry.broadcast(msg);

        // Both handlers should have received the message
        assert_eq!(delivered, 2);
    }

    #[test]
    fn no_duplicate_handlers() {
        use std::any::TypeId;

        struct HandlerActor {
            id: ActorId,
            handles: Vec<TypeId>,
        }

        struct TestMessage;

        impl Actor for HandlerActor {
            fn id(&self) -> ActorId {
                self.id
            }

            fn set_id(&mut self, id: ActorId) {
                self.id = id;
            }

            fn handles(&self) -> &[TypeId] {
                &self.handles
            }

            fn handle(&mut self, _msg: Message, _ctx: &mut ActorContext) -> HandleResult {
                HandleResult::Handled
            }
        }

        let mut registry = ActorRegistry::new();

        let id = registry.spawn(Box::new(HandlerActor {
            id: ActorId::default(),
            handles: vec![TypeId::of::<TestMessage>()],
        }));

        // Try to register the same handler again via context
        let mut ctx = ActorContext::new(id);
        ctx.register_handler(id, TypeId::of::<TestMessage>());
        registry.process_context_requests(ctx);

        // Should still only have one handler
        let handlers = registry.handlers_for(TypeId::of::<TestMessage>());
        assert_eq!(handlers.len(), 1);
    }

    #[test]
    fn deep_actor_hierarchy_despawn() {
        // Test that deep hierarchies don't cause stack overflow (uses iterative despawn)
        let mut registry = ActorRegistry::new();

        // Create a chain of parent -> child -> grandchild etc.
        let root = registry.spawn(Box::new(TestActor::new(0)));
        let mut current = root;

        // Create 100 levels of nesting
        for i in 1..=100 {
            let child = registry.spawn_with_parent(Box::new(TestActor::new(i)), current);
            current = child;
        }

        assert_eq!(registry.len(), 101);

        // Despawn root should despawn all 101 actors
        registry.despawn(root);

        assert_eq!(registry.len(), 0);
        let metrics = registry.metrics().snapshot();
        assert_eq!(metrics.actors_despawned, 101);
    }

    #[test]
    fn send_result_types() {
        let mut registry = ActorRegistry::new();
        let actor_id = registry.spawn(Box::new(TestActor::new(1)));

        // Create a message to a non-existent actor
        let mut actors: slotmap::SlotMap<ActorId, ()> = slotmap::SlotMap::with_key();
        let fake_source = actors.insert(());
        let fake_target = actors.insert(());

        let msg = Message::new(fake_target, 42i32, fake_source);
        let result = registry.send(msg);

        // Should be ActorNotFound
        assert!(matches!(result, SendResult::ActorNotFound(_)));

        // Cleanup
        registry.despawn(actor_id);
    }

    #[test]
    fn supervision_restart_resets_actor() {
        use std::sync::atomic::{AtomicU32, Ordering};

        static SPAWN_COUNT: AtomicU32 = AtomicU32::new(0);
        static DESPAWN_COUNT: AtomicU32 = AtomicU32::new(0);
        static RESET_COUNT: AtomicU32 = AtomicU32::new(0);

        SPAWN_COUNT.store(0, Ordering::SeqCst);
        DESPAWN_COUNT.store(0, Ordering::SeqCst);
        RESET_COUNT.store(0, Ordering::SeqCst);

        struct ResettableActor {
            id: ActorId,
            parent_id: Option<ActorId>,
            handles: Vec<TypeId>,
        }

        struct ErrorMessage;
        struct SupervisorActor {
            id: ActorId,
        }

        impl Actor for ResettableActor {
            fn id(&self) -> ActorId { self.id }
            fn set_id(&mut self, id: ActorId) { self.id = id; }
            fn handles(&self) -> &[TypeId] { &self.handles }
            fn parent(&self) -> Option<ActorId> { self.parent_id }
            fn set_parent(&mut self, parent: Option<ActorId>) { self.parent_id = parent; }

            fn handle(&mut self, msg: Message, _ctx: &mut ActorContext) -> HandleResult {
                if msg.is::<ErrorMessage>() {
                    HandleResult::Error(ActorError::Internal("test error".into()))
                } else {
                    HandleResult::NotHandled
                }
            }

            fn on_spawn(&mut self, _ctx: &mut ActorContext) {
                SPAWN_COUNT.fetch_add(1, Ordering::SeqCst);
            }

            fn on_despawn(&mut self, _ctx: &mut ActorContext) {
                DESPAWN_COUNT.fetch_add(1, Ordering::SeqCst);
            }

            fn reset(&mut self) {
                RESET_COUNT.fetch_add(1, Ordering::SeqCst);
            }

            fn type_name(&self) -> &'static str { "ResettableActor" }
        }

        impl Actor for SupervisorActor {
            fn id(&self) -> ActorId { self.id }
            fn set_id(&mut self, id: ActorId) { self.id = id; }
            fn handles(&self) -> &[TypeId] { &[] }
            fn handle(&mut self, _msg: Message, _ctx: &mut ActorContext) -> HandleResult {
                HandleResult::NotHandled
            }

            fn on_child_error(
                &mut self,
                _child: ActorId,
                _error: &ActorError,
                _ctx: &mut ActorContext,
            ) -> SupervisionStrategy {
                SupervisionStrategy::Restart
            }

            fn type_name(&self) -> &'static str { "SupervisorActor" }
        }

        let mut registry = ActorRegistry::new();

        let supervisor_id = registry.spawn(Box::new(SupervisorActor {
            id: ActorId::default(),
        }));

        let child_id = registry.spawn_with_parent(
            Box::new(ResettableActor {
                id: ActorId::default(),
                parent_id: None,
                handles: vec![TypeId::of::<ErrorMessage>()],
            }),
            supervisor_id,
        );

        assert_eq!(SPAWN_COUNT.load(Ordering::SeqCst), 1); // only ResettableActor counts

        // Send an error-causing message to the child.
        let msg = Message::new(child_id, ErrorMessage, child_id);
        let _ = registry.send(msg);
        registry.tick();

        // The child should have been restarted: on_despawn, reset, on_spawn called.
        assert_eq!(DESPAWN_COUNT.load(Ordering::SeqCst), 1, "on_despawn called");
        assert_eq!(RESET_COUNT.load(Ordering::SeqCst), 1, "reset called");
        assert_eq!(SPAWN_COUNT.load(Ordering::SeqCst), 2, "on_spawn called again");

        // The child should still exist.
        assert!(registry.get(child_id).is_some());
        assert_eq!(registry.len(), 2);
    }
}
