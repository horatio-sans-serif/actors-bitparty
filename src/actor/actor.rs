use std::any::TypeId;

use slotmap::new_key_type;
use thiserror::Error;

use super::context::ActorContext;
use super::message::Message;

new_key_type! {
    /// Unique identifier for an actor in the system.
    /// Uses slotmap for efficient O(1) lookup and generation tracking.
    pub struct ActorId;
}

/// Errors that can occur during message handling.
#[derive(Debug, Clone, Error)]
pub enum ActorError {
    /// The message type is not supported by this actor.
    #[error("unsupported message type: {0}")]
    UnsupportedMessage(String),

    /// An internal error occurred during processing.
    #[error("internal error: {0}")]
    Internal(String),

    /// A required resource was not found.
    #[error("resource not found: {0}")]
    ResourceNotFound(String),

    /// The actor is in an invalid state for this operation.
    #[error("invalid state: {0}")]
    InvalidState(String),

    /// A child actor reported an error (for supervision).
    #[error("child actor {actor:?} failed: {error}")]
    ChildError {
        actor: ActorId,
        error: Box<ActorError>,
    },
}

/// Result of handling a message.
#[must_use = "handle result indicates success/failure and should be checked"]
#[derive(Debug, Clone)]
pub enum HandleResult {
    /// Message was handled successfully.
    Handled,
    /// Message was not handled (actor doesn't handle this type).
    NotHandled,
    /// Message handling failed with an error.
    Error(ActorError),
}

impl HandleResult {
    /// Create an error result from a string message.
    pub fn error(msg: impl Into<String>) -> Self {
        Self::Error(ActorError::Internal(msg.into()))
    }

    /// Check if the result is an error.
    pub fn is_error(&self) -> bool {
        matches!(self, Self::Error(_))
    }

    /// Check if the message was handled (either success or error).
    pub fn was_handled(&self) -> bool {
        !matches!(self, Self::NotHandled)
    }
}

impl PartialEq for HandleResult {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (HandleResult::Handled, HandleResult::Handled) => true,
            (HandleResult::NotHandled, HandleResult::NotHandled) => true,
            (HandleResult::Error(e1), HandleResult::Error(e2)) => e1.to_string() == e2.to_string(),
            _ => false,
        }
    }
}

impl Eq for HandleResult {}

/// Supervision strategy for handling child actor errors.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SupervisionStrategy {
    /// Resume the failed actor without any action.
    Resume,
    /// Restart the failed actor.
    Restart,
    /// Stop the failed actor.
    Stop,
    /// Escalate the error to the parent supervisor.
    Escalate,
}

impl Default for SupervisionStrategy {
    fn default() -> Self {
        Self::Resume
    }
}

/// The core trait that all actors must implement.
///
/// An actor is a self-contained unit that:
/// - Has a unique identity (ActorId)
/// - Receives and handles messages
/// - Can send messages to other actors
/// - Can have parent/child relationships with other actors
///
/// # Thread Safety
///
/// Actors must be `Send + Sync` because:
/// - The actor registry may be shared across threads
/// - Message passing can occur from any thread via InboxHandle
/// - The actor system may be shared across threads
pub trait Actor: Send + Sync {
    /// Returns this actor's unique identifier.
    fn id(&self) -> ActorId;

    /// Sets this actor's identifier. Called by the registry during spawn.
    fn set_id(&mut self, id: ActorId);

    /// Returns the message types this actor can handle.
    /// Used by MessageExchange for routing broadcasts.
    fn handles(&self) -> &[TypeId];

    /// Handle an incoming message.
    ///
    /// # Arguments
    /// * `msg` - The message to handle
    /// * `ctx` - Context providing access to system services
    ///
    /// # Returns
    /// Result indicating whether the message was handled.
    fn handle(&mut self, msg: Message, ctx: &mut ActorContext) -> HandleResult;

    /// Returns the IDs of this actor's children.
    fn children(&self) -> &[ActorId] {
        &[]
    }

    /// Returns the ID of this actor's parent, if any.
    fn parent(&self) -> Option<ActorId> {
        None
    }

    /// Sets the parent of this actor.
    fn set_parent(&mut self, _parent: Option<ActorId>) {}

    /// Called when this actor is spawned into the system.
    fn on_spawn(&mut self, _ctx: &mut ActorContext) {}

    /// Called when this actor is about to be despawned.
    fn on_despawn(&mut self, _ctx: &mut ActorContext) {}

    /// Reset this actor to its initial state (for supervision restart).
    ///
    /// Called by the supervisor when `SupervisionStrategy::Restart` is chosen.
    /// The actor should clear any transient state while preserving identity
    /// and configuration. After reset, `on_spawn` will be called again.
    ///
    /// Default implementation does nothing (acts like Resume).
    fn reset(&mut self) {}

    /// Called when a child actor reports an error.
    ///
    /// The default implementation returns `Escalate` to propagate
    /// errors up the supervision hierarchy.
    fn on_child_error(
        &mut self,
        _child: ActorId,
        _error: &ActorError,
        _ctx: &mut ActorContext,
    ) -> SupervisionStrategy {
        SupervisionStrategy::Escalate
    }

    /// Returns the name of this actor type (for debugging).
    fn type_name(&self) -> &'static str {
        std::any::type_name::<Self>()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn handle_result_equality() {
        assert_eq!(HandleResult::Handled, HandleResult::Handled);
        assert_eq!(HandleResult::NotHandled, HandleResult::NotHandled);
        assert_eq!(
            HandleResult::error("test"),
            HandleResult::error("test")
        );
        assert_ne!(HandleResult::Handled, HandleResult::NotHandled);
    }

    #[test]
    fn actor_error_display() {
        let err = ActorError::Internal("test error".into());
        assert_eq!(err.to_string(), "internal error: test error");

        let err = ActorError::UnsupportedMessage("FooMsg".into());
        assert_eq!(err.to_string(), "unsupported message type: FooMsg");
    }

    #[test]
    fn handle_result_helpers() {
        assert!(HandleResult::error("test").is_error());
        assert!(!HandleResult::Handled.is_error());
        assert!(!HandleResult::NotHandled.is_error());

        assert!(HandleResult::Handled.was_handled());
        assert!(HandleResult::error("test").was_handled());
        assert!(!HandleResult::NotHandled.was_handled());
    }
}
