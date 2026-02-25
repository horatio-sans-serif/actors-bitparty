use std::any::TypeId;
use std::collections::HashMap;

use smallvec::SmallVec;

use super::actor::{Actor, ActorId, HandleResult};
use super::context::ActorContext;
use super::message::Message;

/// MessageExchange is a special actor that routes broadcast messages.
///
/// When an actor sends a message with `target: None`, the system finds
/// the nearest MessageExchange in the actor hierarchy and uses it to
/// broadcast the message to all actors that handle that message type.
///
/// This provides a pub/sub pattern where actors can communicate without
/// needing to know each other's IDs. It's similar to event bubbling in
/// traditional UI frameworks, but completely generic.
///
/// # Example Hierarchy
///
/// ```text
/// Root (MessageExchange)
///   ├── Panel A
///   │   ├── Button (broadcasts "Click")
///   │   └── Label (handles "Click")
///   └── Panel B (MessageExchange)
///       ├── Button (broadcasts "Click")
///       └── Label (handles "Click")
/// ```
///
/// In this example:
/// - Panel A's Button broadcasts to Root's MessageExchange
/// - Panel B's Button broadcasts to Panel B's MessageExchange (nearest)
/// - Messages stay scoped to their nearest exchange
pub struct MessageExchange {
    id: ActorId,
    parent: Option<ActorId>,
    children: SmallVec<[ActorId; 8]>,
    /// Mapping from message type to actors that handle it.
    handlers: HashMap<TypeId, SmallVec<[ActorId; 8]>>,
    /// Whether to propagate unhandled messages to parent exchange.
    propagate_unhandled: bool,
}

impl MessageExchange {
    /// Create a new message exchange.
    pub fn new() -> Self {
        Self {
            id: ActorId::default(),
            parent: None,
            children: SmallVec::new(),
            handlers: HashMap::new(),
            propagate_unhandled: false,
        }
    }

    /// Create a new message exchange that propagates unhandled messages.
    pub fn with_propagation() -> Self {
        Self {
            propagate_unhandled: true,
            ..Self::new()
        }
    }

    /// Maximum handlers per message type.
    pub const MAX_HANDLERS_PER_TYPE: usize = 256;

    /// Register a handler for a message type.
    ///
    /// Returns `true` if the handler was registered, `false` if:
    /// - The handler is already registered for this type
    /// - The maximum handlers per type limit has been reached
    pub fn register_handler(&mut self, msg_type: TypeId, actor: ActorId) -> bool {
        let handlers = self.handlers.entry(msg_type).or_default();
        if handlers.contains(&actor) {
            return false; // Already registered
        }
        if handlers.len() >= Self::MAX_HANDLERS_PER_TYPE {
            log::warn!(
                "MessageExchange {:?}: max handlers reached for message type",
                self.id
            );
            return false;
        }
        handlers.push(actor);
        true
    }

    /// Unregister a handler for a message type.
    pub fn unregister_handler(&mut self, msg_type: TypeId, actor: ActorId) {
        if let Some(handlers) = self.handlers.get_mut(&msg_type) {
            handlers.retain(|id| *id != actor);
        }
    }

    /// Unregister all handlers for an actor.
    pub fn unregister_actor(&mut self, actor: ActorId) {
        for handlers in self.handlers.values_mut() {
            handlers.retain(|id| *id != actor);
        }
    }

    /// Get handlers for a message type.
    pub fn handlers_for(&self, msg_type: TypeId) -> &[ActorId] {
        self.handlers
            .get(&msg_type)
            .map(|v| v.as_slice())
            .unwrap_or(&[])
    }

    /// Maximum children per MessageExchange.
    pub const MAX_CHILDREN: usize = 1024;

    /// Add a child actor.
    ///
    /// Returns `true` if the child was added, `false` if:
    /// - The child is already present
    /// - The maximum children limit has been reached
    pub fn add_child(&mut self, child: ActorId) -> bool {
        if self.children.contains(&child) {
            return false; // Already a child
        }
        if self.children.len() >= Self::MAX_CHILDREN {
            log::warn!(
                "MessageExchange {:?}: max children capacity reached",
                self.id
            );
            return false;
        }
        self.children.push(child);
        true
    }

    /// Remove a child actor.
    pub fn remove_child(&mut self, child: ActorId) {
        self.children.retain(|id| *id != child);
    }
}

impl Default for MessageExchange {
    fn default() -> Self {
        Self::new()
    }
}

impl Actor for MessageExchange {
    fn id(&self) -> ActorId {
        self.id
    }

    fn set_id(&mut self, id: ActorId) {
        self.id = id;
    }

    fn handles(&self) -> &[TypeId] {
        // MessageExchange handles all message types (it's a router)
        // but we return empty because we don't want to be registered
        // as a handler for specific types - we're a meta-handler.
        &[]
    }

    fn handle(&mut self, msg: Message, ctx: &mut ActorContext) -> HandleResult {
        // Route broadcast messages to registered handlers
        // Note: handlers_for returns a reference, so we need to collect to avoid borrow issues
        let handlers: SmallVec<[ActorId; 8]> = self
            .handlers
            .get(&msg.payload_type)
            .cloned()
            .unwrap_or_default();

        if handlers.is_empty() {
            if self.propagate_unhandled {
                // Propagate to parent exchange
                if let Some(parent) = self.parent {
                    ctx.send_raw(msg.retarget(parent));
                    return HandleResult::Handled;
                }
            }
            return HandleResult::NotHandled;
        }

        // Route to ALL handlers using Arc-based message sharing
        // Message is cloned using Arc internally, so this is efficient
        for &handler_id in &handlers {
            if handler_id != msg.source {
                // Clone the message (Arc-based, efficient sharing)
                let cloned = msg.clone().retarget(handler_id);
                ctx.send_raw(cloned);
            }
        }

        HandleResult::Handled
    }

    fn children(&self) -> &[ActorId] {
        &self.children
    }

    fn parent(&self) -> Option<ActorId> {
        self.parent
    }

    fn set_parent(&mut self, parent: Option<ActorId>) {
        self.parent = parent;
    }

    fn type_name(&self) -> &'static str {
        "MessageExchange"
    }
}

/// Message to register a handler with an exchange.
#[derive(Debug, Clone)]
pub struct RegisterHandler {
    pub msg_type: TypeId,
    pub actor: ActorId,
}

/// Message to unregister a handler from an exchange.
#[derive(Debug, Clone)]
pub struct UnregisterHandler {
    pub msg_type: TypeId,
    pub actor: ActorId,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn register_and_lookup_handler() {
        let mut exchange = MessageExchange::new();
        let msg_type = TypeId::of::<i32>();

        // Create a fake actor id using slotmap
        use slotmap::SlotMap;
        let mut actors: SlotMap<ActorId, ()> = SlotMap::with_key();
        let handler_id = actors.insert(());

        exchange.register_handler(msg_type, handler_id);
        assert_eq!(exchange.handlers_for(msg_type), &[handler_id]);

        exchange.unregister_handler(msg_type, handler_id);
        assert!(exchange.handlers_for(msg_type).is_empty());
    }

    #[test]
    fn unregister_actor_removes_all_handlers() {
        let mut exchange = MessageExchange::new();

        use slotmap::SlotMap;
        let mut actors: SlotMap<ActorId, ()> = SlotMap::with_key();
        let handler_id = actors.insert(());

        exchange.register_handler(TypeId::of::<i32>(), handler_id);
        exchange.register_handler(TypeId::of::<String>(), handler_id);

        exchange.unregister_actor(handler_id);

        assert!(exchange.handlers_for(TypeId::of::<i32>()).is_empty());
        assert!(exchange.handlers_for(TypeId::of::<String>()).is_empty());
    }
}
