use std::any::TypeId;

use super::actor::ActorId;
use super::mailbox::InboxHandle;
use super::message::{Message, MessagePayload};

/// Context provided to actors during message handling.
///
/// This is the actor's view of the system - it provides access to:
/// - Sending messages to other actors
/// - Spawning/despawning actors
/// - Querying the actor hierarchy
///
/// Note: Context queues have maximum capacities to prevent unbounded memory growth.
/// If a capacity is exceeded, the operation returns false or is ignored.
pub struct ActorContext {
    /// The ID of the actor being processed.
    pub current_actor: ActorId,
    /// Outgoing messages to be routed after handling.
    outgoing: Vec<Message>,
    /// Actors to be spawned after handling.
    spawn_requests: Vec<SpawnRequest>,
    /// Actors to be despawned after handling.
    despawn_requests: Vec<ActorId>,
    /// Inbox handles for direct message delivery.
    inbox_handles: Vec<(ActorId, InboxHandle)>,
    /// Handler registrations (actor_id, message_type).
    handlers: Vec<(ActorId, TypeId)>,
}

impl std::fmt::Debug for ActorContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ActorContext")
            .field("current_actor", &self.current_actor)
            .field("outgoing_count", &self.outgoing.len())
            .field("spawn_requests_count", &self.spawn_requests.len())
            .field("despawn_requests_count", &self.despawn_requests.len())
            .field("inbox_handles_count", &self.inbox_handles.len())
            .field("handlers_count", &self.handlers.len())
            .finish()
    }
}

impl ActorContext {
    /// Maximum outgoing messages per context.
    pub const MAX_OUTGOING: usize = 1024;
    /// Maximum spawn requests per context.
    pub const MAX_SPAWN_REQUESTS: usize = 256;
    /// Maximum despawn requests per context.
    pub const MAX_DESPAWN_REQUESTS: usize = 256;
    /// Maximum inbox handle registrations per context.
    pub const MAX_INBOX_HANDLES: usize = 256;
    /// Maximum handler registrations per context.
    pub const MAX_HANDLER_REGISTRATIONS: usize = 256;
}

/// Request to spawn a new actor.
pub struct SpawnRequest {
    pub actor: Box<dyn FnOnce() -> Box<dyn super::Actor>>,
    pub parent: Option<ActorId>,
}

impl std::fmt::Debug for SpawnRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SpawnRequest")
            .field("parent", &self.parent)
            .field("actor", &"<factory closure>")
            .finish()
    }
}

impl ActorContext {
    /// Create a new context for the given actor.
    pub fn new(current_actor: ActorId) -> Self {
        Self {
            current_actor,
            outgoing: Vec::new(),
            spawn_requests: Vec::new(),
            despawn_requests: Vec::new(),
            inbox_handles: Vec::new(),
            handlers: Vec::new(),
        }
    }

    /// Send a message to a specific actor.
    ///
    /// Returns `true` if the message was queued, `false` if the outgoing queue is full.
    pub fn send<T: MessagePayload>(&mut self, target: ActorId, payload: T) -> bool {
        if self.outgoing.len() >= Self::MAX_OUTGOING {
            log::warn!(
                "Outgoing message queue full for actor {:?}, dropping message",
                self.current_actor
            );
            return false;
        }
        let msg = Message::new(target, payload, self.current_actor);
        self.outgoing.push(msg);
        true
    }

    /// Broadcast a message (routed via MessageExchange).
    ///
    /// Returns `true` if the message was queued, `false` if the outgoing queue is full.
    pub fn broadcast<T: MessagePayload>(&mut self, payload: T) -> bool {
        if self.outgoing.len() >= Self::MAX_OUTGOING {
            log::warn!(
                "Outgoing message queue full for actor {:?}, dropping broadcast",
                self.current_actor
            );
            return false;
        }
        let msg = Message::broadcast(payload, self.current_actor);
        self.outgoing.push(msg);
        true
    }

    /// Queue a raw message for sending.
    ///
    /// Returns `true` if the message was queued, `false` if the outgoing queue is full.
    pub fn send_raw(&mut self, msg: Message) -> bool {
        if self.outgoing.len() >= Self::MAX_OUTGOING {
            log::warn!(
                "Outgoing message queue full for actor {:?}, dropping raw message",
                self.current_actor
            );
            return false;
        }
        self.outgoing.push(msg);
        true
    }

    /// Request to spawn a new actor.
    ///
    /// The actor will be spawned after the current message handling completes.
    /// Returns `true` if the request was queued, `false` if the spawn queue is full.
    pub fn spawn<F, A>(&mut self, factory: F) -> bool
    where
        F: FnOnce() -> A + 'static,
        A: super::Actor + 'static,
    {
        if self.spawn_requests.len() >= Self::MAX_SPAWN_REQUESTS {
            log::warn!(
                "Spawn request queue full for actor {:?}, ignoring spawn",
                self.current_actor
            );
            return false;
        }
        self.spawn_requests.push(SpawnRequest {
            actor: Box::new(move || Box::new(factory())),
            parent: None,
        });
        true
    }

    /// Request to spawn a new actor as a child of the current actor.
    ///
    /// Returns `true` if the request was queued, `false` if the spawn queue is full.
    pub fn spawn_child<F, A>(&mut self, factory: F) -> bool
    where
        F: FnOnce() -> A + 'static,
        A: super::Actor + 'static,
    {
        if self.spawn_requests.len() >= Self::MAX_SPAWN_REQUESTS {
            log::warn!(
                "Spawn request queue full for actor {:?}, ignoring spawn_child",
                self.current_actor
            );
            return false;
        }
        self.spawn_requests.push(SpawnRequest {
            actor: Box::new(move || Box::new(factory())),
            parent: Some(self.current_actor),
        });
        true
    }

    /// Request to despawn an actor.
    ///
    /// Returns `true` if the request was queued, `false` if the despawn queue is full.
    pub fn despawn(&mut self, id: ActorId) -> bool {
        if self.despawn_requests.len() >= Self::MAX_DESPAWN_REQUESTS {
            log::warn!(
                "Despawn request queue full for actor {:?}, ignoring despawn of {:?}",
                self.current_actor,
                id
            );
            return false;
        }
        self.despawn_requests.push(id);
        true
    }

    /// Request to despawn the current actor (self-destruct).
    ///
    /// Returns `true` if the request was queued, `false` if the despawn queue is full.
    pub fn despawn_self(&mut self) -> bool {
        if self.despawn_requests.len() >= Self::MAX_DESPAWN_REQUESTS {
            log::warn!(
                "Despawn request queue full for actor {:?}, ignoring despawn_self",
                self.current_actor
            );
            return false;
        }
        self.despawn_requests.push(self.current_actor);
        true
    }

    /// Register an inbox handle for direct message delivery.
    ///
    /// Returns `true` if registered, `false` if the inbox handle queue is full.
    pub fn register_inbox(&mut self, id: ActorId, handle: InboxHandle) -> bool {
        if self.inbox_handles.len() >= Self::MAX_INBOX_HANDLES {
            log::warn!(
                "Inbox handle registration queue full for actor {:?}",
                self.current_actor
            );
            return false;
        }
        self.inbox_handles.push((id, handle));
        true
    }

    /// Register a message type handler for an actor.
    ///
    /// Returns `true` if registered, `false` if the handler registration queue is full.
    pub fn register_handler(&mut self, id: ActorId, msg_type: TypeId) -> bool {
        if self.handlers.len() >= Self::MAX_HANDLER_REGISTRATIONS {
            log::warn!(
                "Handler registration queue full for actor {:?}",
                self.current_actor
            );
            return false;
        }
        self.handlers.push((id, msg_type));
        true
    }

    /// Take all outgoing messages.
    pub fn take_outgoing(&mut self) -> Vec<Message> {
        std::mem::take(&mut self.outgoing)
    }

    /// Take all spawn requests.
    pub fn take_spawn_requests(&mut self) -> Vec<SpawnRequest> {
        std::mem::take(&mut self.spawn_requests)
    }

    /// Take all despawn requests.
    pub fn take_despawn_requests(&mut self) -> Vec<ActorId> {
        std::mem::take(&mut self.despawn_requests)
    }

    /// Take all inbox handle registrations.
    pub fn take_inbox_handles(&mut self) -> Vec<(ActorId, InboxHandle)> {
        std::mem::take(&mut self.inbox_handles)
    }

    /// Take all handler registrations.
    pub fn take_handlers(&mut self) -> Vec<(ActorId, TypeId)> {
        std::mem::take(&mut self.handlers)
    }

    /// Check if there are any pending operations.
    pub fn has_pending(&self) -> bool {
        !self.outgoing.is_empty()
            || !self.spawn_requests.is_empty()
            || !self.despawn_requests.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use slotmap::SlotMap;

    #[test]
    fn context_send_message() {
        let mut actors: SlotMap<ActorId, ()> = SlotMap::with_key();
        let current = actors.insert(());
        let target = actors.insert(());

        let mut ctx = ActorContext::new(current);
        ctx.send(target, 42i32);

        let outgoing = ctx.take_outgoing();
        assert_eq!(outgoing.len(), 1);
        assert_eq!(outgoing[0].target, Some(target));
        assert_eq!(outgoing[0].source, current);
    }

    #[test]
    fn context_broadcast() {
        let mut actors: SlotMap<ActorId, ()> = SlotMap::with_key();
        let current = actors.insert(());

        let mut ctx = ActorContext::new(current);
        ctx.broadcast("hello");

        let outgoing = ctx.take_outgoing();
        assert_eq!(outgoing.len(), 1);
        assert!(outgoing[0].target.is_none());
    }

    #[test]
    fn context_despawn_self() {
        let mut actors: SlotMap<ActorId, ()> = SlotMap::with_key();
        let current = actors.insert(());

        let mut ctx = ActorContext::new(current);
        ctx.despawn_self();

        let despawns = ctx.take_despawn_requests();
        assert_eq!(despawns.len(), 1);
        assert_eq!(despawns[0], current);
    }
}
