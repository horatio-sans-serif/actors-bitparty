use std::any::{Any, TypeId};
use std::fmt;
use std::sync::Arc;

use super::actor::ActorId;

/// Marker trait for message payloads.
pub trait MessagePayload: Any + Send + Sync {}

/// Blanket implementation for all types that satisfy the bounds.
impl<T: Any + Send + Sync> MessagePayload for T {}

/// Shared payload wrapper for broadcast messages.
///
/// Uses Arc internally to allow efficient sharing across multiple recipients.
/// When a message is cloned (e.g., for broadcasting), the payload is shared
/// via Arc rather than deeply copied, making broadcast operations efficient.
///
/// # Thread Safety
///
/// This type is always constructed via `SharedPayload::new<T>()` which
/// requires `T: Any + Send + Sync`, ensuring thread-safety of the inner value.
/// The Arc provides atomic reference counting for safe concurrent access.
///
/// # Performance
///
/// Cloning a SharedPayload is O(1) - it only increments the Arc reference count.
#[derive(Clone)]
pub struct SharedPayload {
    inner: Arc<dyn Any + Send + Sync>,
    type_id: TypeId,
    type_name: &'static str,
}

impl SharedPayload {
    /// Create a new shared payload.
    pub fn new<T: MessagePayload>(payload: T) -> Self {
        Self {
            inner: Arc::new(payload),
            type_id: TypeId::of::<T>(),
            type_name: std::any::type_name::<T>(),
        }
    }

    /// Try to downcast to a specific type.
    pub fn downcast_ref<T: 'static>(&self) -> Option<&T> {
        self.inner.downcast_ref::<T>()
    }

    /// Check if the payload is of a specific type.
    pub fn is<T: 'static>(&self) -> bool {
        self.type_id == TypeId::of::<T>()
    }

    /// Get the type ID.
    pub fn type_id(&self) -> TypeId {
        self.type_id
    }

    /// Get the type name for debugging.
    pub fn type_name(&self) -> &'static str {
        self.type_name
    }
}

impl fmt::Debug for SharedPayload {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SharedPayload({})", self.type_name)
    }
}

/// A message sent between actors.
///
/// Messages can be either targeted (sent to a specific actor) or broadcast
/// (sent to all actors that handle the message type).
///
/// For broadcast messages, the payload uses Arc-based sharing to efficiently
/// deliver the same message to multiple recipients without cloning the data.
pub struct Message {
    /// Target actor. If `None`, the message is a broadcast
    /// that will be routed through a MessageExchange.
    pub target: Option<ActorId>,

    /// The type ID of the payload, used for routing.
    pub payload_type: TypeId,

    /// The actual message content stored as Arc for sharing.
    payload: SharedPayload,

    /// The actor that sent this message.
    pub source: ActorId,
}

impl Clone for Message {
    fn clone(&self) -> Self {
        Self {
            target: self.target,
            payload_type: self.payload_type,
            payload: self.payload.clone(),
            source: self.source,
        }
    }
}

impl Message {
    /// Create a new targeted message.
    pub fn new<T: MessagePayload>(target: ActorId, payload: T, source: ActorId) -> Self {
        let shared = SharedPayload::new(payload);
        Self {
            target: Some(target),
            payload_type: shared.type_id(),
            payload: shared,
            source,
        }
    }

    /// Create a new broadcast message (no specific target).
    pub fn broadcast<T: MessagePayload>(payload: T, source: ActorId) -> Self {
        let shared = SharedPayload::new(payload);
        Self {
            target: None,
            payload_type: shared.type_id(),
            payload: shared,
            source,
        }
    }

    /// Try to downcast the payload to a specific type.
    pub fn downcast_ref<T: 'static>(&self) -> Option<&T> {
        self.payload.downcast_ref::<T>()
    }

    /// Check if the payload is of a specific type.
    pub fn is<T: 'static>(&self) -> bool {
        self.payload_type == TypeId::of::<T>()
    }

    /// Get the type name of the payload (for debugging).
    pub fn payload_type_name(&self) -> &'static str {
        self.payload.type_name()
    }

    /// Retarget this message to a specific actor.
    /// Consumes the message and returns a new one with the new target.
    pub fn retarget(self, target: ActorId) -> Self {
        Self {
            target: Some(target),
            payload_type: self.payload_type,
            payload: self.payload,
            source: self.source,
        }
    }

    /// Convert to a broadcast message (no specific target).
    pub fn into_broadcast(self) -> Self {
        Self {
            target: None,
            payload_type: self.payload_type,
            payload: self.payload,
            source: self.source,
        }
    }
}

impl fmt::Debug for Message {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Message")
            .field("target", &self.target)
            .field("payload_type", &self.payload.type_name())
            .field("source", &self.source)
            .finish()
    }
}

// Common message types

/// Request to close/despawn an actor.
#[derive(Debug, Clone)]
pub struct CloseRequest;

/// Notification that an actor was spawned.
#[derive(Debug, Clone)]
pub struct ActorSpawned {
    pub id: ActorId,
}

/// Notification that an actor was despawned.
#[derive(Debug, Clone)]
pub struct ActorDespawned {
    pub id: ActorId,
}

/// Request a redraw of the UI.
#[derive(Debug, Clone)]
pub struct RedrawRequest;

/// Focus request - actor wants input focus.
#[derive(Debug, Clone)]
pub struct FocusRequest {
    pub actor: ActorId,
}

/// Focus changed notification.
#[derive(Debug, Clone)]
pub struct FocusChanged {
    pub previous: Option<ActorId>,
    pub current: Option<ActorId>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use slotmap::SlotMap;

    #[derive(Debug, Clone, PartialEq)]
    struct TestPayload {
        value: i32,
    }

    #[test]
    fn message_creation() {
        let mut actors: SlotMap<ActorId, ()> = SlotMap::with_key();
        let source = actors.insert(());
        let target = actors.insert(());

        let msg = Message::new(target, TestPayload { value: 42 }, source);

        assert_eq!(msg.target, Some(target));
        assert_eq!(msg.source, source);
        assert!(msg.is::<TestPayload>());
    }

    #[test]
    fn message_downcast() {
        let mut actors: SlotMap<ActorId, ()> = SlotMap::with_key();
        let source = actors.insert(());
        let target = actors.insert(());

        let msg = Message::new(target, TestPayload { value: 42 }, source);

        let payload = msg.downcast_ref::<TestPayload>().unwrap();
        assert_eq!(payload.value, 42);
    }

    #[test]
    fn broadcast_message() {
        let mut actors: SlotMap<ActorId, ()> = SlotMap::with_key();
        let source = actors.insert(());

        let msg = Message::broadcast(TestPayload { value: 100 }, source);

        assert!(msg.target.is_none());
        assert!(msg.is::<TestPayload>());
    }

    #[test]
    fn message_clone_shares_payload() {
        let mut actors: SlotMap<ActorId, ()> = SlotMap::with_key();
        let source = actors.insert(());

        let msg1 = Message::broadcast(TestPayload { value: 42 }, source);
        let msg2 = msg1.clone();

        // Both should have the same payload
        let p1 = msg1.downcast_ref::<TestPayload>().unwrap();
        let p2 = msg2.downcast_ref::<TestPayload>().unwrap();
        assert_eq!(p1.value, p2.value);
    }
}
