//! Bridge message types for cross-runtime communication.
//!
//! These types are serializable (serde) and use WorldObjectId (UUID) for
//! stable cross-runtime addressing. They form the wire format for the
//! TypeScript <-> Rust protocol bridge.

use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Stable cross-runtime identity for world objects.
/// Persists across process restarts. Stored in SQLite.
/// Maps to runtime-local ActorId within each runtime.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct WorldObjectId(pub Uuid);

impl WorldObjectId {
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }

    pub fn from_uuid(uuid: Uuid) -> Self {
        Self(uuid)
    }

    pub fn as_uuid(&self) -> &Uuid {
        &self.0
    }
}

impl Default for WorldObjectId {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for WorldObjectId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Which plane a bridge message operates on.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum BridgePlane {
    Management,
    Data,
}

/// Priority level for bridge messages.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum BridgePriority {
    Normal,
    High,
}

impl Default for BridgePriority {
    fn default() -> Self {
        Self::Normal
    }
}

/// A message that crosses the TypeScript <-> Rust bridge.
/// Newline-delimited JSON over WebSocket or Unix domain socket.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BridgeMessage {
    pub id: Uuid,
    pub plane: BridgePlane,
    #[serde(rename = "type")]
    pub msg_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub from: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub to: Option<String>,
    pub payload: serde_json::Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub authorization: Option<String>,
    #[serde(default)]
    pub priority: BridgePriority,
    pub timestamp: u64,
}

impl BridgeMessage {
    pub fn management(msg_type: impl Into<String>, payload: serde_json::Value) -> Self {
        Self {
            id: Uuid::new_v4(),
            plane: BridgePlane::Management,
            msg_type: msg_type.into(),
            from: None,
            to: None,
            payload,
            authorization: None,
            priority: BridgePriority::Normal,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
        }
    }

    pub fn data(
        msg_type: impl Into<String>,
        from: WorldObjectId,
        to: WorldObjectId,
        payload: serde_json::Value,
    ) -> Self {
        Self {
            id: Uuid::new_v4(),
            plane: BridgePlane::Data,
            msg_type: msg_type.into(),
            from: Some(from.to_string()),
            to: Some(to.to_string()),
            payload,
            authorization: None,
            priority: BridgePriority::Normal,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
        }
    }

    /// Serialize to newline-delimited JSON (one line, terminated with \n).
    pub fn to_ndjson(&self) -> Result<String, serde_json::Error> {
        let mut json = serde_json::to_string(self)?;
        json.push('\n');
        Ok(json)
    }

    /// Deserialize from a JSON line (with or without trailing newline).
    pub fn from_ndjson(line: &str) -> Result<Self, serde_json::Error> {
        serde_json::from_str(line.trim())
    }
}

/// Handshake message sent by the Rust process on startup.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReadyMessage {
    #[serde(rename = "type")]
    pub msg_type: String,
    pub version: String,
}

impl Default for ReadyMessage {
    fn default() -> Self {
        Self {
            msg_type: "ready".to_string(),
            version: "1.0".to_string(),
        }
    }
}

/// Object type for placed world objects.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ObjectType {
    Static,
    Interactive,
    ProjectAnchor,
    Npc,
}

/// A placement record as stored in SQLite and exchanged over the bridge.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Placement {
    pub id: WorldObjectId,
    pub world_id: Uuid,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub project_id: Option<Uuid>,
    pub position: [f32; 3],
    pub rotation: [f32; 4],
    pub scale: [f32; 3],
    pub mesh_asset: String,
    pub object_type: ObjectType,
    #[serde(default)]
    pub bindings: serde_json::Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub conjure_prompt: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn world_object_id_roundtrip() {
        let id = WorldObjectId::new();
        let json = serde_json::to_string(&id).unwrap();
        let parsed: WorldObjectId = serde_json::from_str(&json).unwrap();
        assert_eq!(id, parsed);
    }

    #[test]
    fn bridge_message_ndjson_roundtrip() {
        let msg = BridgeMessage::management(
            "spawn-object",
            serde_json::json!({
                "mesh": "assets/meshes/desk.glb",
                "position": [1.0, 0.0, 3.0]
            }),
        );
        let ndjson = msg.to_ndjson().unwrap();
        assert!(ndjson.ends_with('\n'));
        assert!(!ndjson[..ndjson.len() - 1].contains('\n'));

        let parsed = BridgeMessage::from_ndjson(&ndjson).unwrap();
        assert_eq!(parsed.id, msg.id);
        assert_eq!(parsed.msg_type, "spawn-object");
        assert_eq!(parsed.plane, BridgePlane::Management);
    }

    #[test]
    fn data_plane_message() {
        let from = WorldObjectId::new();
        let to = WorldObjectId::new();
        let msg = BridgeMessage::data(
            "interact",
            from,
            to,
            serde_json::json!({"action": "open"}),
        );
        assert_eq!(msg.plane, BridgePlane::Data);
        assert_eq!(msg.from.unwrap(), from.to_string());
        assert_eq!(msg.to.unwrap(), to.to_string());
    }

    #[test]
    fn placement_serialization() {
        let placement = Placement {
            id: WorldObjectId::new(),
            world_id: Uuid::new_v4(),
            project_id: None,
            position: [1.0, 2.0, 3.0],
            rotation: [0.0, 0.0, 0.0, 1.0],
            scale: [1.0, 1.0, 1.0],
            mesh_asset: "assets/meshes/desk.glb".to_string(),
            object_type: ObjectType::Interactive,
            bindings: serde_json::json!({}),
            conjure_prompt: Some("wooden desk with monitors".to_string()),
        };
        let json = serde_json::to_string(&placement).unwrap();
        let parsed: Placement = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.id, placement.id);
        assert_eq!(parsed.object_type, ObjectType::Interactive);
        assert_eq!(parsed.conjure_prompt, placement.conjure_prompt);
    }

    #[test]
    fn ready_message() {
        let ready = ReadyMessage::default();
        let json = serde_json::to_string(&ready).unwrap();
        assert!(json.contains("\"type\":\"ready\""));
        assert!(json.contains("\"version\":\"1.0\""));
    }
}
