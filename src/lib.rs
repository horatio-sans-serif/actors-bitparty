//! BitParty - Actor-based framework with message passing
//!
//! A framework where everything is an actor with lock-free message passing.
//! Designed for real-time applications where performance matters.
//!
//! # Core Concepts
//!
//! - **Actor**: A self-contained unit with an inbox/outbox and message handlers
//! - **Message**: Type-safe communication between actors
//! - **MessageExchange**: Routing for broadcast messages (pub/sub pattern)
//!
//! # Features
//!
//! - `serde`: Serialization support for bridge-crossing messages

pub mod actor;

/// Bridge message types for cross-runtime communication.
/// Requires the `serde` feature.
#[cfg(feature = "serde")]
pub mod bridge;

/// Prelude module for convenient imports.
pub mod prelude {
    pub use crate::actor::{
        Actor, ActorContext, ActorId, ActorRegistry, HandleResult, Mailbox, Message,
        MessageExchange, MessagePayload,
    };
}

#[cfg(test)]
mod tests {
    use crate::actor::{
        Actor, ActorContext, ActorId, ActorRegistry, HandleResult, Message, MessagePayload,
    };
    use std::any::TypeId;

    #[derive(Debug, Clone)]
    struct Ping;

    #[derive(Debug, Clone)]
    struct Pong;

    struct EchoActor {
        id: ActorId,
        received: Vec<String>,
        handles: Vec<TypeId>,
    }

    impl EchoActor {
        fn new() -> Self {
            Self {
                id: ActorId::default(),
                received: Vec::new(),
                handles: vec![TypeId::of::<Ping>()],
            }
        }
    }

    impl Actor for EchoActor {
        fn id(&self) -> ActorId {
            self.id
        }

        fn set_id(&mut self, id: ActorId) {
            self.id = id;
        }

        fn handles(&self) -> &[TypeId] {
            &self.handles
        }

        fn handle(&mut self, msg: Message, ctx: &mut ActorContext) -> HandleResult {
            if msg.is::<Ping>() {
                self.received.push("Ping".to_string());
                ctx.send(msg.source, Pong);
                HandleResult::Handled
            } else {
                HandleResult::NotHandled
            }
        }
    }

    #[test]
    fn basic_actor_system() {
        let mut registry = ActorRegistry::new();

        let actor1 = registry.spawn(Box::new(EchoActor::new()));
        let actor2 = registry.spawn(Box::new(EchoActor::new()));

        assert_eq!(registry.len(), 2);
        assert!(registry.get(actor1).is_some());
        assert!(registry.get(actor2).is_some());
    }

}
