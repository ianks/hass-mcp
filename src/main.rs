use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use strum_macros::{AsRefStr, Display};

use rmcp::{
    Error as McpError, RoleServer, ServerHandler, ServiceExt, model::*, service::RequestContext,
};
use schemars::{JsonSchema, SchemaGenerator};
use serde::{Deserialize, Serialize};
// use rmcp::handler::server::tool::schema_for_type; // Not using rmcp's schemars anymore

// Our own schema_for_type using schemars 0.9 with JSON Schema draft 2020-12 settings
fn schema_for_type<T: JsonSchema>() -> serde_json::Map<String, serde_json::Value> {
    // Use default settings for schemars 0.9 which should generate proper schemas
    let schema = SchemaGenerator::default().into_root_schema_for::<T>();
    let object = serde_json::to_value(schema).expect("failed to serialize schema");
    match object {
        serde_json::Value::Object(object) => object,
        _ => panic!("unexpected schema value"),
    }
}
use clap::Parser;
use futures_util::{SinkExt, StreamExt};
use fuzzy_matcher::FuzzyMatcher;
use reqwest::Client;
use rmcp::transport::stdio;
use serde_json::json;
use tokio::net::TcpStream;
use tokio::sync::{Mutex, mpsc, oneshot};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async, tungstenite::Message};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

// =============================================================================
// Core Configuration & Client Abstractions
// =============================================================================

use url::Url;

pub struct HomeAssistantConfig {
    pub url: String,
    pub token: String,
}

impl HomeAssistantConfig {
    pub fn websocket_url(&self) -> String {
        let mut url = Url::parse(&self.url).expect("Invalid URL");
        let new_scheme = match url.scheme() {
            "https" => "wss",
            "http" => "ws",
            _ => "ws", // Default to ws for unsupported schemes
        };
        url.set_scheme(new_scheme).unwrap(); // Safe because we control the scheme
        url.set_path("/api/websocket");
        url.to_string()
    }
    pub fn new(url: String, token: String) -> Self {
        Self { url, token }
    }

    // Old websocket_url function removed
}

// =============================================================================
// Enums and Argument Structs with JSON Schema
// =============================================================================

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, Display, AsRefStr)]
#[schemars(title = "Home Assistant Resource")]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum HaResource {
    AreaRegistry,
    DeviceRegistry,
    EntityRegistry,
    FloorRegistry,
    LabelRegistry,
    StateObject,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, Display, AsRefStr)]
#[schemars(title = "Entity Domain")]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum EntityDomain {
    Sensor,
    Light,
    Switch,
    BinarySensor,
    Camera,
    Climate,
    Cover,
    Fan,
    Lock,
    MediaPlayer,
    Vacuum,
    WaterHeater,
    Weather,
    DeviceTracker,
    Person,
    Zone,
    Sun,
    InputBoolean,
    InputNumber,
    InputSelect,
    InputText,
    InputDatetime,
    Timer,
    Counter,
    Automation,
    Script,
    Scene,
    Group,
    Other,
}

// Registry operation argument structs
#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct RegistryListArgs {
    /// The Home Assistant resource to list
    pub resource: HaResource,
    /// Maximum number of items to return (default: 50)
    #[serde(default)]
    pub limit: Option<usize>,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct RegistryCreateArgs {
    /// The Home Assistant resource to create in
    pub resource: HaResource,
    /// The data for the new item (JSON object)
    #[schemars(with = "std::collections::HashMap<String, serde_json::Value>")]
    pub data: serde_json::Value,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct RegistryUpdateArgs {
    /// The Home Assistant resource to update in
    pub resource: HaResource,
    /// The ID of the item to update
    pub id: String,
    /// The changes to apply (JSON object)
    #[schemars(with = "std::collections::HashMap<String, serde_json::Value>")]
    pub changes: serde_json::Value,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct RegistryDeleteArgs {
    /// The Home Assistant resource to delete from
    pub resource: HaResource,
    /// The ID of the item to delete
    pub id: String,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct DescribeResourceArgs {
    /// The Home Assistant resource to describe
    pub resource: HaResource,
}

// Entity operation argument structs
#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct EntityFindArgs {
    /// Filter by entity domain (e.g., sensor, light)
    #[serde(default)]
    pub domain: Option<EntityDomain>,
    /// Filter by area ID
    #[serde(default)]
    pub area_id: Option<String>,
    /// Filter by device ID
    #[serde(default)]
    pub device_id: Option<String>,
    /// Filter entities that have labels (true) or no labels (false)
    #[serde(default)]
    pub has_labels: Option<bool>,
    /// Maximum number of entities to return (default: 100)
    #[serde(default)]
    pub limit: Option<usize>,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct EntityGetArgs {
    /// The entity ID to retrieve
    pub entity_id: String,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct EntityUpdateArgs {
    /// The entity ID to update
    pub entity_id: String,
    /// New friendly name for the entity
    #[serde(default)]
    pub name: Option<String>,
    /// Area ID to assign the entity to
    #[serde(default)]
    pub area_id: Option<String>,
    /// Disable the entity (set to "user" to disable, null to enable)
    #[serde(default)]
    pub disabled_by: Option<String>,
    /// Labels to assign to the entity (replaces existing)
    #[serde(default)]
    pub labels: Option<Vec<String>>,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct EntityAssignAreaArgs {
    /// The entity ID to assign
    pub entity_id: String,
    /// The area ID to assign to
    pub area_id: String,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct EntityAddLabelsArgs {
    /// The entity ID to add labels to
    pub entity_id: String,
    /// Labels to add (preserves existing labels)
    pub labels: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct EntityRemoveLabelsArgs {
    /// The entity ID to remove labels from
    pub entity_id: String,
    /// Specific labels to remove (if None, removes all labels)
    #[serde(default)]
    pub labels: Option<Vec<String>>,
}

// Device operation argument structs
#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct DeviceFindArgs {
    /// Filter by area ID
    pub area_id: Option<String>,
    /// Filter by manufacturer name (case-insensitive contains)
    pub manufacturer: Option<String>,
    /// Maximum number of devices to return (default: 50)
    pub limit: Option<usize>,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct DeviceGetArgs {
    /// The device ID to retrieve
    pub device_id: String,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct DeviceUpdateArgs {
    /// The device ID to update
    pub device_id: String,
    /// New friendly name for the device
    pub name: Option<String>,
    /// Area ID to assign the device to
    pub area_id: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct DeviceGetEntitiesArgs {
    /// The device ID to get entities for
    pub device_id: String,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct DeviceLabelAllEntitiesArgs {
    /// The device ID to label all entities for
    pub device_id: String,
    /// Labels to apply to all entities of this device
    pub labels: Vec<String>,
}

// Discovery and analysis argument structs
#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct FindOrphanedEntitiesArgs {
    /// Maximum number of orphaned entities to return (default: 50)
    #[serde(default)]
    pub limit: Option<usize>,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct RecommendLabelsArgs {
    /// Maximum number of recommendations to return (default: 50)
    #[serde(default)]
    pub limit: Option<usize>,
}

// Label creation argument structs
#[derive(Debug, Serialize, Deserialize, JsonSchema, Clone)]
pub struct CreateLabelArgs {
    /// Label name
    pub name: String,
    /// Label icon (MDI icon name)
    #[serde(default)]
    pub icon: Option<String>,
    /// Label color
    #[serde(default)]
    pub color: Option<String>,
    /// Label description
    #[serde(default)]
    pub description: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct UpdateLabelArgs {
    /// Label ID to update
    pub label_id: String,
    /// Label name
    #[serde(default)]
    pub name: Option<String>,
    /// Label icon (MDI icon name)
    #[serde(default)]
    pub icon: Option<String>,
    /// Label color
    #[serde(default)]
    pub color: Option<String>,
    /// Label description
    #[serde(default)]
    pub description: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct CreateAndLabelDeviceArgs {
    /// Label name
    pub label_name: String,
    /// Label icon (MDI icon name)
    pub icon: Option<String>,
    /// Label color
    pub color: Option<String>,
    /// Label description
    pub description: Option<String>,
    /// Device ID to label
    pub device_id: String,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct CreateAndAssignLabelArgs {
    /// Label name
    pub name: String,
    /// Label icon (MDI icon name)
    pub icon: Option<String>,
    /// Label color
    pub color: Option<String>,
    /// Label description
    pub description: Option<String>,
    /// Entity IDs to assign the label to
    pub entity_ids: Vec<String>,
}

// State analysis argument structs for answering analytical questions
#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct GetAllEntityStatesArgs {
    /// Optional domain filter (e.g., "sensor", "light")
    #[serde(default)]
    pub domain: Option<String>,
    /// Include entity attributes in response
    #[serde(default = "default_true")]
    pub include_attributes: bool,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct GetDeviceStatesArgs {
    /// Device ID to get all entity states for
    pub device_id: String,
    /// Include entity attributes in response
    #[serde(default = "default_true")]
    pub include_attributes: bool,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct AnalyzeZigbeeNetworkArgs {
    /// Include signal strength and LQI data
    #[serde(default = "default_true")]
    pub include_signal_data: bool,
    /// Include route/topology information
    #[serde(default = "default_true")]
    pub include_topology: bool,
}

fn default_true() -> bool {
    true
}

// Additional argument structs for unused methods
#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct ListDevicesArgs {
    /// Filter by area ID
    #[serde(default)]
    pub area_id: Option<String>,
    /// Filter by manufacturer
    #[serde(default)]
    pub manufacturer: Option<String>,
    /// Maximum number of devices to return
    #[serde(default)]
    pub limit: Option<usize>,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct UpdateAreaArgs {
    /// Area ID to update
    pub area_id: String,
    /// New area name
    #[serde(default)]
    pub name: Option<String>,
    /// Floor ID to assign area to
    #[serde(default)]
    pub floor_id: Option<String>,
    /// Area aliases
    #[serde(default)]
    pub aliases: Option<Vec<String>>,
    /// Area icon
    #[serde(default)]
    pub icon: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct UpdateDeviceArgs {
    /// Device ID to update
    pub device_id: String,
    /// New device name
    #[serde(default)]
    pub name: Option<String>,
    /// Area ID to assign device to
    #[serde(default)]
    pub area_id: Option<String>,
    /// Device aliases
    #[serde(default)]
    pub name_by_user: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct UpdateEntityArgs {
    /// Entity ID to update
    pub entity_id: String,
    /// New entity name
    #[serde(default)]
    pub name: Option<String>,
    /// Area ID to assign entity to
    #[serde(default)]
    pub area_id: Option<String>,
    /// Icon for the entity
    #[serde(default)]
    pub icon: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct CreateAreaArgs {
    /// Area name
    pub name: String,
    /// Floor ID to assign area to
    #[serde(default)]
    pub floor_id: Option<String>,
    /// Area aliases
    #[serde(default)]
    pub aliases: Option<Vec<String>>,
    /// Area icon
    #[serde(default)]
    pub icon: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct CreateFloorArgs {
    /// Floor name
    pub name: String,
    /// Floor level
    #[serde(default)]
    pub level: Option<i32>,
    /// Floor aliases
    #[serde(default)]
    pub aliases: Option<Vec<String>>,
    /// Floor icon
    #[serde(default)]
    pub icon: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct DeleteAreaArgs {
    /// Area ID to delete
    pub area_id: String,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct DeleteFloorArgs {
    /// Floor ID to delete
    pub floor_id: String,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct DeleteLabelArgs {
    /// Label ID to delete
    pub label_id: String,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct AssignEntityToAreaArgs {
    /// Entity ID to assign
    pub entity_id: String,
    /// Area ID to assign to
    pub area_id: String,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct UnassignEntityFromAreaArgs {
    /// Entity ID to unassign
    pub entity_id: String,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct AssignDeviceToAreaArgs {
    /// Device ID to assign
    pub device_id: String,
    /// Area ID to assign to
    pub area_id: String,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct AddLabelsToEntityArgs {
    /// Entity ID to add labels to
    pub entity_id: String,
    /// Labels to add
    pub labels: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct ClearEntityLabelsArgs {
    /// Entity ID to clear labels from
    pub entity_id: String,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct GetSummaryArgs {
    /// Include device counts
    #[serde(default = "default_true")]
    pub include_devices: bool,
    /// Include entity counts
    #[serde(default = "default_true")]
    pub include_entities: bool,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct GetEntitiesInAreaArgs {
    /// Area ID
    pub area_id: String,
    /// Domain filter
    #[serde(default)]
    pub domain: Option<EntityDomain>,
    /// Maximum number of entities to return
    #[serde(default)]
    pub limit: Option<usize>,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct GetUnassignedEntitiesArgs {
    /// Domain filter
    #[serde(default)]
    pub domain: Option<EntityDomain>,
    /// Maximum number of entities to return
    #[serde(default)]
    pub limit: Option<usize>,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct FindAreaByNameArgs {
    /// Area name to search for
    pub name: String,
    /// Use fuzzy matching
    #[serde(default = "default_true")]
    pub fuzzy: bool,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct FindFloorByNameArgs {
    /// Floor name to search for
    pub name: String,
    /// Use fuzzy matching
    #[serde(default = "default_true")]
    pub fuzzy: bool,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct SearchEntitiesArgs {
    /// Search query
    pub query: String,
    /// Domain filter
    #[serde(default)]
    pub domain: Option<EntityDomain>,
    /// Use fuzzy matching
    #[serde(default = "default_true")]
    pub fuzzy: bool,
    /// Maximum number of entities to return
    #[serde(default)]
    pub limit: Option<usize>,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct FindEntitiesByLabelArgs {
    /// Label name to search for
    pub label: String,
    /// Domain filter
    #[serde(default)]
    pub domain: Option<EntityDomain>,
    /// Maximum number of entities to return
    #[serde(default)]
    pub limit: Option<usize>,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct DiscoverResourcesArgs {
    /// Include detailed schema information
    #[serde(default = "default_true")]
    pub include_schemas: bool,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct DiscoverToolsArgs {
    /// Include detailed tool descriptions
    #[serde(default = "default_true")]
    pub include_descriptions: bool,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct ValidateEntityIdArgs {
    /// Entity ID to validate
    pub entity_id: String,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct ValidateDeviceIdArgs {
    /// Device ID to validate
    pub device_id: String,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct GetEntitiesForDeviceArgs {
    /// Device ID
    pub device_id: String,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct GetDeviceSummaryArgs {
    /// Device ID
    pub device_id: String,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct AnalyzeConfigurationArgs {
    /// Include recommendations
    #[serde(default = "default_true")]
    pub include_recommendations: bool,
    /// Include statistics
    #[serde(default = "default_true")]
    pub include_statistics: bool,
}

// =============================================================================
// Home Assistant API Client Abstraction
// =============================================================================

// Command sent to the WebSocket manager
#[derive(Debug)]
struct WebSocketCommand {
    id: u64,
    payload: serde_json::Value,
    response_sender: oneshot::Sender<Result<serde_json::Value, McpError>>,
}

// WebSocket manager that handles the persistent connection
struct WebSocketManager {
    ws_stream: WebSocketStream<MaybeTlsStream<TcpStream>>,
    pending_responses: HashMap<u64, oneshot::Sender<Result<serde_json::Value, McpError>>>,
}

impl WebSocketManager {
    async fn new(config: &HomeAssistantConfig) -> Result<Self, McpError> {
        let ws_url = config.websocket_url();
        let (ws_stream, _) = connect_async(&ws_url).await.map_err(|e| {
            McpError::internal_error(format!("Failed to connect to WebSocket: {}", e), None)
        })?;

        let mut manager = Self {
            ws_stream,
            pending_responses: HashMap::new(),
        };

        // Authenticate immediately
        manager.authenticate(config).await?;
        Ok(manager)
    }

    async fn authenticate(&mut self, config: &HomeAssistantConfig) -> Result<(), McpError> {
        let auth_msg = json!({
            "type": "auth",
            "access_token": config.token
        });

        self.ws_stream
            .send(Message::Text(auth_msg.to_string().into()))
            .await
            .map_err(|e| McpError::internal_error(format!("Failed to send auth: {}", e), None))?;

        // Wait for auth response
        while let Some(msg) = self.ws_stream.next().await {
            let msg = msg.map_err(|e| {
                McpError::internal_error(format!("WebSocket error during auth: {}", e), None)
            })?;

            if let Message::Text(text) = msg {
                if let Ok(response) = serde_json::from_str::<serde_json::Value>(&text) {
                    if response.get("type") == Some(&json!("auth_ok")) {
                        return Ok(());
                    } else if response.get("type") == Some(&json!("auth_invalid")) {
                        return Err(McpError::internal_error("Authentication failed", None));
                    }
                }
            }
        }

        Err(McpError::internal_error("No auth response received", None))
    }

    async fn run(
        &mut self,
        mut command_receiver: mpsc::Receiver<WebSocketCommand>,
    ) -> Result<(), McpError> {
        loop {
            tokio::select! {
                // Handle incoming commands
                Some(cmd) = command_receiver.recv() => {
                    self.pending_responses.insert(cmd.id, cmd.response_sender);

                    if let Err(e) = self.ws_stream.send(Message::Text(cmd.payload.to_string().into())).await {
                        // Clean up the pending response and send error
                        if let Some(sender) = self.pending_responses.remove(&cmd.id) {
                            let _ = sender.send(Err(McpError::internal_error(format!("Failed to send command: {}", e), None)));
                        }
                    }
                }

                // Handle incoming WebSocket messages
                Some(msg_result) = self.ws_stream.next() => {
                    match msg_result {
                        Ok(Message::Text(text)) => {
                            if let Ok(response) = serde_json::from_str::<serde_json::Value>(&text) {
                                if let Some(id) = response.get("id").and_then(|v| v.as_u64()) {
                                    if let Some(sender) = self.pending_responses.remove(&id) {
                                        let result = if response.get("success") == Some(&json!(true)) {
                                            response.get("result").cloned()
                                                .ok_or_else(|| McpError::internal_error("No result in successful response", None))
                                        } else {
                                            let error_msg = response.get("error")
                                                .and_then(|e| e.as_str())
                                                .unwrap_or("Unknown error")
                                                .to_string();
                                            Err(McpError::internal_error(error_msg, None))
                                        };
                                        let _ = sender.send(result);
                                    }
                                }
                            }
                        }
                        Ok(Message::Close(_)) => {
                            // WebSocket closed, break the loop
                            break;
                        }
                        Err(e) => {
                            // WebSocket error, break the loop
                            tracing::error!("WebSocket error: {}", e);
                            break;
                        }
                        _ => {
                            // Ignore other message types
                        }
                    }
                }

                // Channel closed
                else => break,
            }
        }

        // Clean up pending responses
        for (_, sender) in self.pending_responses.drain() {
            let _ = sender.send(Err(McpError::internal_error(
                "WebSocket connection closed",
                None,
            )));
        }

        Ok(())
    }
}

pub struct HomeAssistantClient {
    config: HomeAssistantConfig,
    http_client: Client,
    command_sender: mpsc::Sender<WebSocketCommand>,
    command_id_counter: Arc<AtomicU64>,
}

impl HomeAssistantClient {
    pub async fn new(config: HomeAssistantConfig) -> Result<Self, McpError> {
        let (command_sender, command_receiver) = mpsc::channel(100);
        let command_id_counter = Arc::new(AtomicU64::new(1));

        // Create the WebSocket manager
        let mut ws_manager = WebSocketManager::new(&config).await?;

        // Spawn the background task to handle WebSocket communication
        tokio::spawn(async move {
            if let Err(e) = ws_manager.run(command_receiver).await {
                tracing::error!("WebSocket manager error: {}", e);
                // TODO: Add reconnection logic here
            }
        });

        Ok(Self {
            config,
            http_client: Client::new(),
            command_sender,
            command_id_counter,
        })
    }

    pub async fn websocket_command(
        &self,
        mut command: serde_json::Value,
    ) -> Result<serde_json::Value, McpError> {
        // Generate unique command ID
        let id = self.command_id_counter.fetch_add(1, Ordering::SeqCst);
        command["id"] = json!(id);

        // Create response channel
        let (response_sender, response_receiver) = oneshot::channel();

        // Send command to WebSocket manager
        let ws_command = WebSocketCommand {
            id,
            payload: command,
            response_sender,
        };

        self.command_sender
            .send(ws_command)
            .await
            .map_err(|_| McpError::internal_error("WebSocket manager is not running", None))?;

        // Wait for response
        response_receiver
            .await
            .map_err(|_| McpError::internal_error("Failed to receive WebSocket response", None))?
    }

    pub async fn rest_get(&self, endpoint: &str) -> Result<serde_json::Value, McpError> {
        let response = self.http_client
            .get(format!("{}/{}", self.config.url, endpoint))
            .header("Authorization", format!("Bearer {}", self.config.token))
            .send()
            .await
            .map_err(|e| {
                if e.is_connect() {
                    McpError::internal_error(
                        format!("Cannot connect to Home Assistant at '{}'. Please check the URL and ensure Home Assistant is running.", self.config.url), 
                        None
                    )
                } else if e.is_timeout() {
                    McpError::internal_error(
                        format!("Timeout connecting to Home Assistant at '{}'", self.config.url), 
                        None
                    )
                } else {
                    McpError::internal_error(
                        format!("Network error accessing {}: {}", endpoint, e), 
                        None
                    )
                }
            })?;

        if !response.status().is_success() {
            let status = response.status();
            let error_text = response
                .text()
                .await
                .unwrap_or_else(|_| "Unable to read error response".to_string());

            let error_msg = match status.as_u16() {
                401 => "Authentication failed. Please check your HASS_TOKEN is valid and has not expired.".to_string(),
                403 => "Access forbidden. Your HASS_TOKEN may not have sufficient permissions.".to_string(),
                404 => format!("Home Assistant API endpoint not found: {}", endpoint),
                500..=599 => format!("Home Assistant server error ({}): {}", status, error_text),
                _ => format!("HTTP error {} accessing {}: {}", status, endpoint, error_text),
            };

            return Err(McpError::internal_error(error_msg, None));
        }

        response
            .json()
            .await
            .map_err(|e| McpError::internal_error(format!("Failed to parse JSON: {}", e), None))
    }
}

// =============================================================================
// Registry Operations Abstraction
// =============================================================================

pub struct RegistryOperations {
    client: HomeAssistantClient,
}

impl RegistryOperations {
    pub fn new(client: HomeAssistantClient) -> Self {
        Self { client }
    }

    pub async fn list(&self, registry: &str) -> Result<serde_json::Value, McpError> {
        let query = json!({ "id": 1, "type": format!("config/{}/list", registry) });
        self.client.websocket_command(query).await
    }

    pub async fn update(
        &self,
        registry: &str,
        item_id: &str,
        updates: serde_json::Value,
    ) -> Result<(), McpError> {
        let mut query = json!({
            "id": 1,
            "type": format!("config/{}/update", registry),
            format!("{}_id", registry.replace("_registry", "")): item_id
        });

        // Add update fields directly to the command (only non-null values)
        for (key, value) in updates.as_object().unwrap_or(&serde_json::Map::new()) {
            if !value.is_null() {
                query[key] = value.clone();
            }
        }

        self.client.websocket_command(query).await?;
        Ok(())
    }

    pub async fn create(
        &self,
        registry: &str,
        data: serde_json::Value,
    ) -> Result<serde_json::Value, McpError> {
        let mut query = json!({
            "id": 1,
            "type": format!("config/{}/create", registry)
        });

        // Add creation fields directly to the command (only non-null values)
        for (key, value) in data.as_object().unwrap_or(&serde_json::Map::new()) {
            if !value.is_null() {
                query[key] = value.clone();
            }
        }

        self.client.websocket_command(query).await
    }

    pub async fn delete(&self, registry: &str, item_id: &str) -> Result<(), McpError> {
        let query = json!({
            "id": 1,
            "type": format!("config/{}/delete", registry),
            format!("{}_id", registry.replace("_registry", "")): item_id
        });

        self.client.websocket_command(query).await?;
        Ok(())
    }

    pub async fn get_states(
        &self,
        filter_prefix: Option<&str>,
    ) -> Result<Vec<serde_json::Value>, McpError> {
        let states = self.client.rest_get("api/states").await?;

        let empty_vec = vec![];
        let mut filtered_states: Vec<serde_json::Value> =
            states.as_array().unwrap_or(&empty_vec).to_vec();

        // Apply filter if specified
        if let Some(prefix) = filter_prefix {
            filtered_states.retain(|state| {
                state
                    .get("entity_id")
                    .and_then(|id| id.as_str())
                    .map(|id| id.starts_with(prefix))
                    .unwrap_or(false)
            });
        }

        Ok(filtered_states)
    }
}

// =============================================================================
// Entity Domain Model & Operations Abstraction
// =============================================================================

#[derive(Debug, Clone)]
pub struct Entity {
    pub entity_id: String,
    pub name: Option<String>,
    pub device_id: Option<String>,
    pub area_id: Option<String>,
    pub labels: Vec<String>,
    pub disabled_by: Option<String>,
    pub domain: String,
}

impl Entity {
    pub fn from_json(value: &serde_json::Value) -> Option<Self> {
        let entity_id: String = value.get("entity_id")?.as_str()?.into();
        let domain: String = entity_id.split('.').next()?.into();

        Some(Entity {
            entity_id,
            name: value.get("name").and_then(|v| v.as_str()).map(|s| s.into()),
            device_id: value
                .get("device_id")
                .and_then(|v| v.as_str())
                .map(|s| s.into()),
            area_id: value
                .get("area_id")
                .and_then(|v| v.as_str())
                .map(|s| s.into()),
            labels: value
                .get("labels")
                .and_then(|v| v.as_array())
                .map(|arr| {
                    arr.iter()
                        .filter_map(|l| l.as_str().map(|s| s.into()))
                        .collect()
                })
                .unwrap_or_default(),
            disabled_by: value
                .get("disabled_by")
                .and_then(|v| v.as_str())
                .map(|s| s.into()),
            domain,
        })
    }

    pub fn is_unlabeled(&self) -> bool {
        self.labels.is_empty()
    }

    pub fn is_unassigned(&self) -> bool {
        self.area_id.is_none() || self.area_id.as_ref().map(|s| s.is_empty()).unwrap_or(true)
    }

    pub fn is_disabled(&self) -> bool {
        self.disabled_by.is_some()
    }

    pub fn has_label(&self, label: &str) -> bool {
        self.labels.contains(&label.into())
    }

    pub fn matches_domain(&self, domain: &str) -> bool {
        self.domain == domain
    }
}

pub struct EntityOperations {
    registry_ops: Arc<RegistryOperations>,
}

impl EntityOperations {
    pub fn new(registry_ops: Arc<RegistryOperations>) -> Self {
        Self { registry_ops }
    }

    pub async fn list_all(&self) -> Result<Vec<Entity>, McpError> {
        let result = self.registry_ops.list("entity_registry").await?;
        Ok(self.parse_entities(&result))
    }

    pub async fn find_by_id(&self, entity_id: &str) -> Result<Option<Entity>, McpError> {
        let entities = self.list_all().await?;
        Ok(entities.into_iter().find(|e| e.entity_id == entity_id))
    }

    pub async fn find_by_device(&self, device_id: &str) -> Result<Vec<Entity>, McpError> {
        let entities = self.list_all().await?;
        Ok(entities
            .into_iter()
            .filter(|e| {
                e.device_id
                    .as_ref()
                    .map(|d| d == device_id)
                    .unwrap_or(false)
            })
            .collect())
    }

    pub async fn find_by_area(&self, area_id: &str) -> Result<Vec<Entity>, McpError> {
        let entities = self.list_all().await?;
        Ok(entities
            .into_iter()
            .filter(|e| e.area_id.as_ref().map(|a| a == area_id).unwrap_or(false))
            .collect())
    }

    pub async fn find_by_domain(&self, domain: &str) -> Result<Vec<Entity>, McpError> {
        let entities = self.list_all().await?;
        Ok(entities
            .into_iter()
            .filter(|e| e.matches_domain(domain))
            .collect())
    }

    pub async fn find_by_label(&self, label: &str) -> Result<Vec<Entity>, McpError> {
        let entities = self.list_all().await?;
        Ok(entities
            .into_iter()
            .filter(|e| e.has_label(label))
            .collect())
    }

    pub async fn find_unlabeled(&self, limit: Option<usize>) -> Result<Vec<Entity>, McpError> {
        let entities = self.list_all().await?;
        let mut unlabeled: Vec<Entity> =
            entities.into_iter().filter(|e| e.is_unlabeled()).collect();

        if let Some(limit) = limit {
            unlabeled.truncate(limit);
        }

        Ok(unlabeled)
    }

    pub async fn find_unassigned(&self, limit: Option<usize>) -> Result<Vec<Entity>, McpError> {
        let entities = self.list_all().await?;
        let mut unassigned: Vec<Entity> =
            entities.into_iter().filter(|e| e.is_unassigned()).collect();

        if let Some(limit) = limit {
            unassigned.truncate(limit);
        }

        Ok(unassigned)
    }

    pub async fn search_by_pattern(
        &self,
        pattern: &str,
        limit: Option<usize>,
    ) -> Result<Vec<Entity>, McpError> {
        let entities = self.list_all().await?;
        let pattern_lower = pattern.to_lowercase();
        let max_results = limit.unwrap_or(20);

        let matching: Vec<Entity> = entities
            .into_iter()
            .filter(|entity| {
                let entity_id_match = entity.entity_id.to_lowercase().contains(&pattern_lower);
                let name_match = entity
                    .name
                    .as_ref()
                    .map(|name| name.to_lowercase().contains(&pattern_lower))
                    .unwrap_or(false);
                entity_id_match || name_match
            })
            .take(max_results)
            .collect();

        Ok(matching)
    }

    pub async fn update_labels(
        &self,
        entity_id: &str,
        labels: Vec<String>,
    ) -> Result<(), McpError> {
        self.registry_ops
            .update(
                "entity_registry",
                entity_id,
                json!({
                    "labels": labels
                }),
            )
            .await
    }

    pub async fn assign_to_area(
        &self,
        entity_id: &str,
        area_id: Option<String>,
    ) -> Result<(), McpError> {
        self.registry_ops
            .update(
                "entity_registry",
                entity_id,
                json!({
                    "area_id": area_id
                }),
            )
            .await
    }

    pub async fn update_entity(
        &self,
        entity_id: &str,
        updates: serde_json::Value,
    ) -> Result<(), McpError> {
        self.registry_ops
            .update("entity_registry", entity_id, updates)
            .await
    }

    pub async fn label_device_entities(
        &self,
        device_id: &str,
        labels: Vec<String>,
    ) -> Result<(usize, Vec<String>), McpError> {
        let device_entities = self.find_by_device(device_id).await?;

        if device_entities.is_empty() {
            return Ok((0, vec!["No entities found for device".into()]));
        }

        let mut success_count = 0;
        let mut errors = Vec::new();

        for entity in &device_entities {
            match self.update_labels(&entity.entity_id, labels.clone()).await {
                Ok(_) => success_count += 1,
                Err(e) => errors.push(format!("Failed to label {}: {}", entity.entity_id, e)),
            }
        }

        Ok((success_count, errors))
    }

    fn parse_entities(&self, data: &serde_json::Value) -> Vec<Entity> {
        data.as_array()
            .unwrap_or(&vec![])
            .iter()
            .filter_map(Entity::from_json)
            .collect()
    }

    pub fn group_by_domain<'a>(&self, entities: &'a [Entity]) -> HashMap<String, Vec<&'a Entity>> {
        let mut groups: HashMap<String, Vec<&'a Entity>> = HashMap::new();
        for entity in entities {
            groups
                .entry(entity.domain.clone())
                .or_default()
                .push(entity);
        }
        groups
    }

    pub fn group_by_device<'a>(
        &self,
        entities: &'a [Entity],
    ) -> HashMap<Option<String>, Vec<&'a Entity>> {
        let mut groups: HashMap<Option<String>, Vec<&'a Entity>> = HashMap::new();
        for entity in entities {
            groups
                .entry(entity.device_id.clone())
                .or_default()
                .push(entity);
        }
        groups
    }
}

// =============================================================================
// Service Layer Abstraction
// =============================================================================

#[derive(Clone)]
pub struct HomeAssistantService {
    schemas: Arc<Mutex<serde_yaml::Value>>,
    registry_ops: Arc<RegistryOperations>,
}

impl HomeAssistantService {
    pub async fn new(
        schemas: serde_yaml::Value,
        config: HomeAssistantConfig,
    ) -> Result<Self, McpError> {
        let client = HomeAssistantClient::new(config).await?;
        let registry_ops = Arc::new(RegistryOperations::new(client));

        let service = Self {
            schemas: Arc::new(Mutex::new(schemas)),
            registry_ops,
        };

        // Debug print all schemas if RUST_LOG=debug
        if tracing::enabled!(tracing::Level::DEBUG) {
            service.debug_print_all_schemas();
        }

        Ok(service)
    }

    /// Debug function to print all generated JSON schemas
    fn debug_print_all_schemas(&self) {
        tracing::debug!("=== JSON Schema Debug Information ===");

        // Print schemas for all argument structs
        let schemas_to_debug = vec![
            ("RegistryListArgs", schema_for_type::<RegistryListArgs>()),
            (
                "RegistryCreateArgs",
                schema_for_type::<RegistryCreateArgs>(),
            ),
            (
                "RegistryUpdateArgs",
                schema_for_type::<RegistryUpdateArgs>(),
            ),
            (
                "RegistryDeleteArgs",
                schema_for_type::<RegistryDeleteArgs>(),
            ),
            (
                "DescribeResourceArgs",
                schema_for_type::<DescribeResourceArgs>(),
            ),
            ("EntityFindArgs", schema_for_type::<EntityFindArgs>()),
            ("EntityGetArgs", schema_for_type::<EntityGetArgs>()),
            ("EntityUpdateArgs", schema_for_type::<EntityUpdateArgs>()),
            (
                "EntityAssignAreaArgs",
                schema_for_type::<EntityAssignAreaArgs>(),
            ),
            (
                "EntityAddLabelsArgs",
                schema_for_type::<EntityAddLabelsArgs>(),
            ),
            (
                "EntityRemoveLabelsArgs",
                schema_for_type::<EntityRemoveLabelsArgs>(),
            ),
            ("DeviceFindArgs", schema_for_type::<DeviceFindArgs>()),
            ("DeviceGetArgs", schema_for_type::<DeviceGetArgs>()),
            ("DeviceUpdateArgs", schema_for_type::<DeviceUpdateArgs>()),
            (
                "DeviceGetEntitiesArgs",
                schema_for_type::<DeviceGetEntitiesArgs>(),
            ),
            (
                "DeviceLabelAllEntitiesArgs",
                schema_for_type::<DeviceLabelAllEntitiesArgs>(),
            ),
            (
                "FindOrphanedEntitiesArgs",
                schema_for_type::<FindOrphanedEntitiesArgs>(),
            ),
            (
                "RecommendLabelsArgs",
                schema_for_type::<RecommendLabelsArgs>(),
            ),
            ("CreateLabelArgs", schema_for_type::<CreateLabelArgs>()),
            (
                "CreateAndLabelDeviceArgs",
                schema_for_type::<CreateAndLabelDeviceArgs>(),
            ),
            (
                "CreateAndAssignLabelArgs",
                schema_for_type::<CreateAndAssignLabelArgs>(),
            ),
            ("HaResource", schema_for_type::<HaResource>()),
            ("EntityDomain", schema_for_type::<EntityDomain>()),
        ];

        for (name, schema) in schemas_to_debug {
            let schema_json = serde_json::to_string_pretty(&schema)
                .unwrap_or_else(|e| format!("Failed to serialize: {}", e));

            tracing::debug!("Schema for {}: {}", name, schema_json);

            // Also check for problematic type arrays in optional fields
            if schema_json.contains(r#""type":["#) {
                tracing::warn!(
                    "⚠️  Schema for {} contains problematic type array that may not be compatible with JSON Schema draft 2020-12",
                    name
                );
            }

            if schema_json.contains(r#""nullable":true"#) {
                tracing::debug!(
                    "✅ Schema for {} uses nullable:true (good for draft 2020-12)",
                    name
                );
            }
        }

        tracing::debug!("=== End JSON Schema Debug Information ===");
    }

    // =============================================================================
    // Schema & Documentation Helpers
    // =============================================================================

    /// Get Home Assistant specific context for a resource
    fn get_resource_context(&self, resource: &str) -> String {
        match resource {
            "area_registry" => "Areas represent physical locations in your home (e.g., Kitchen, Living Room). They can be assigned to floors and contain devices/entities. Areas are hierarchical organizational units.".into(),
            "device_registry" => "Devices represent physical hardware (e.g., smart switches, sensors, hubs). Each device can have multiple entities and belongs to an area. Devices have manufacturer info and connection details.".into(),
            "entity_registry" => "Entities are the core controllable/monitorable units in HA (e.g., sensor.temperature, light.kitchen). Each entity belongs to a device and can be assigned to areas and labeled.".into(),
            "floor_registry" => "Floors represent different levels of your building. Areas can be assigned to floors for hierarchical organization. Floors have level numbers for proper ordering.".into(),
            "label_registry" => "Labels are tags for organizing entities. They support colors, icons, and descriptions. Use labels for functional grouping (e.g., 'security', 'climate', 'lighting').".into(),
            "state_object" => "State objects represent the current state and attributes of entities. Contains current value, timestamps, and contextual information. Read-only via REST API.".into(),
            _ => "Unknown resource type. Use list_all_resources() to see available resources.".into(),
        }
    }

    /// Get related tools for a resource
    fn get_related_tools(&self, resource: &str) -> String {
        match resource {
            "area_registry" => "Tools: list_areas, create_area, update_area, delete_area, find_area_by_name, assign_entity_to_area, assign_device_to_area, get_entities_in_area".into(),
            "device_registry" => "Tools: list_devices, update_device, assign_device_to_area, get_entities_for_device, label_device, create_and_label_device".into(),
            "entity_registry" => "Tools: list_entities, update_entity, assign_entity_to_area, add_labels_to_entity, clear_entity_labels, search_entities, find_entities_by_label".into(),
            "floor_registry" => "Tools: list_floors, create_floor, delete_floor, find_floor_by_name".into(),
            "label_registry" => "Tools: list_labels, create_label, update_label, delete_label, create_and_assign_label, add_labels_to_entity, find_entities_by_label".into(),
            "state_object" => "Tools: list_sensors, get_entity_state, get_state_history (use REST API for states)".into(),
            _ => "Use discover_tools() to see all available tools.".into(),
        }
    }
}

impl HomeAssistantService {
    async fn describe_resource(
        &self,
        args: DescribeResourceArgs,
    ) -> Result<CallToolResult, McpError> {
        let resource = match args.resource {
            HaResource::AreaRegistry => "area_registry",
            HaResource::DeviceRegistry => "device_registry",
            HaResource::EntityRegistry => "entity_registry",
            HaResource::FloorRegistry => "floor_registry",
            HaResource::LabelRegistry => "label_registry",
            HaResource::StateObject => "state_object",
        };
        let schemas = self.schemas.lock().await;

        // Try exact match first
        if let Some(schema) = schemas.get(resource) {
            let pretty_json = serde_json::to_string_pretty(schema)
                .map_err(|e| McpError::internal_error(e.to_string(), None))?;

            // Add Home Assistant specific context
            let context = self.get_resource_context(resource);

            return Ok(CallToolResult::success(vec![Content::text(format!(
                "# {} Schema\n\n## Fields\n```json\n{}\n```\n\n## Context\n{}\n\n## Related Tools\n{}",
                resource,
                pretty_json,
                context,
                self.get_related_tools(resource)
            ))]));
        }

        // Get available resources
        let available_resources: Vec<String> = schemas
            .as_mapping()
            .unwrap_or(&serde_yaml::Mapping::new())
            .keys()
            .filter_map(|k| k.as_str().map(|s| s.into()))
            .collect();

        // Use fuzzy matching to find the best match
        let matcher = fuzzy_matcher::skim::SkimMatcherV2::default();
        let mut matches: Vec<(String, i64)> = available_resources
            .iter()
            .filter_map(|name| {
                matcher
                    .fuzzy_match(name, resource)
                    .map(|score| (name.clone(), score))
            })
            .collect();

        // Sort by score (highest first)
        matches.sort_by(|a, b| b.1.cmp(&a.1));

        // If we have a good match (score > 50), use it
        if let Some((best_match, score)) = matches.first() {
            if *score > 50 {
                if let Some(schema) = schemas.get(best_match) {
                    let pretty_json = serde_json::to_string_pretty(schema)
                        .map_err(|e| McpError::internal_error(e.to_string(), None))?;
                    let context = self.get_resource_context(best_match);
                    return Ok(CallToolResult::success(vec![Content::text(format!(
                        "Found closest match: '{}' (score: {})\n\n# {} Schema\n\n## Fields\n```json\n{}\n```\n\n## Context\n{}\n\n## Related Tools\n{}",
                        best_match,
                        score,
                        best_match,
                        pretty_json,
                        context,
                        self.get_related_tools(best_match)
                    ))]));
                }
            }
        }

        // No good match found, provide suggestions
        let suggestions = matches
            .into_iter()
            .take(3)
            .map(|(name, score)| format!("{} ({})", name, score))
            .collect::<Vec<_>>()
            .join(", ");

        Ok(CallToolResult::success(vec![Content::text(format!(
            "Resource '{}' not found.\n\nAvailable resources: {}\n\nDid you mean: {}?",
            resource,
            available_resources.join(", "),
            suggestions
        ))]))
    }

    // =============================================================================
    // Helper Methods for Tool Implementations
    // =============================================================================

    /// Helper to format JSON results consistently
    fn format_json_result(&self, data: &serde_json::Value) -> Result<CallToolResult, McpError> {
        let pretty_json = serde_json::to_string_pretty(data)
            .map_err(|e| McpError::internal_error(e.to_string(), None))?;
        Ok(CallToolResult::success(vec![Content::text(pretty_json)]))
    }

    /// Helper to get entities as array with empty fallback
    #[allow(dead_code)]
    fn get_entities_array<'a>(&self, data: &'a serde_json::Value) -> &'a Vec<serde_json::Value> {
        static EMPTY_VEC: Vec<serde_json::Value> = Vec::new();
        data.as_array().unwrap_or(&EMPTY_VEC)
    }

    /// Filter entities by domain
    #[allow(dead_code)]
    fn filter_entities_by_domain<'a>(
        &self,
        entities: &'a [serde_json::Value],
        domain: &str,
    ) -> Vec<&'a serde_json::Value> {
        entities
            .iter()
            .filter(|entity| {
                entity
                    .get("entity_id")
                    .and_then(|id| id.as_str())
                    .map(|id| id.starts_with(&format!("{}.", domain)))
                    .unwrap_or(false)
            })
            .collect()
    }

    /// Filter entities by area
    #[allow(dead_code)]
    fn filter_entities_by_area<'a>(
        &self,
        entities: &'a [serde_json::Value],
        area_id: &str,
    ) -> Vec<&'a serde_json::Value> {
        entities
            .iter()
            .filter(|entity| {
                entity
                    .get("area_id")
                    .and_then(|id| id.as_str())
                    .map(|id| id == area_id)
                    .unwrap_or(false)
            })
            .collect()
    }

    // =============================================================================
    // MCP Tool Implementations
    // =============================================================================

    async fn registry_list(&self, args: RegistryListArgs) -> Result<CallToolResult, McpError> {
        let resource = match args.resource {
            HaResource::AreaRegistry => "area_registry",
            HaResource::DeviceRegistry => "device_registry",
            HaResource::EntityRegistry => "entity_registry",
            HaResource::FloorRegistry => "floor_registry",
            HaResource::LabelRegistry => "label_registry",
            HaResource::StateObject => "state_object",
        };
        let mut result = self.registry_ops.list(resource).await?;

        // Apply limit if specified
        let limit = args.limit.unwrap_or(50);
        if let Some(array) = result.as_array_mut() {
            array.truncate(limit);
        }

        self.format_json_result(&result)
    }

    async fn list_devices(&self, args: ListDevicesArgs) -> Result<CallToolResult, McpError> {
        let result = self.registry_ops.list("device_registry").await?;
        let empty_vec = vec![];
        let mut devices: Vec<&serde_json::Value> =
            result.as_array().unwrap_or(&empty_vec).iter().collect();

        // Apply area filter
        if let Some(area_filter) = &args.area_id {
            devices.retain(|device| {
                device
                    .get("area_id")
                    .and_then(|id| id.as_str())
                    .map(|id| id == area_filter)
                    .unwrap_or(false)
            });
        }

        // Apply manufacturer filter
        if let Some(mfg_filter) = &args.manufacturer {
            let mfg_lower = mfg_filter.to_lowercase();
            devices.retain(|device| {
                device
                    .get("manufacturer")
                    .and_then(|mfg| mfg.as_str())
                    .map(|mfg| mfg.to_lowercase().contains(&mfg_lower))
                    .unwrap_or(false)
            });
        }

        // Apply limit
        if let Some(limit_count) = args.limit {
            devices.truncate(limit_count);
        } else {
            // Default limit to prevent token overflow
            devices.truncate(50);
        }

        let pretty_json = serde_json::to_string_pretty(&devices)
            .map_err(|e| McpError::internal_error(e.to_string(), None))?;
        Ok(CallToolResult::success(vec![Content::text(pretty_json)]))
    }

    async fn entity_find(&self, args: EntityFindArgs) -> Result<CallToolResult, McpError> {
        let result = self.registry_ops.list("entity_registry").await?;
        let empty_vec = vec![];
        let mut entities: Vec<&serde_json::Value> =
            result.as_array().unwrap_or(&empty_vec).iter().collect();

        // Apply domain filter
        if let Some(domain_filter) = &args.domain {
            entities.retain(|entity| {
                entity
                    .get("entity_id")
                    .and_then(|id| id.as_str())
                    .map(|id| id.starts_with(&format!("{}.", domain_filter)))
                    .unwrap_or(false)
            });
        }

        // Apply area filter
        if let Some(area_filter) = &args.area_id {
            entities.retain(|entity| {
                entity
                    .get("area_id")
                    .and_then(|id| id.as_str())
                    .map(|id| id == area_filter)
                    .unwrap_or(false)
            });
        }

        // Apply device filter
        if let Some(device_filter) = &args.device_id {
            entities.retain(|entity| {
                entity
                    .get("device_id")
                    .and_then(|id| id.as_str())
                    .map(|id| id == device_filter)
                    .unwrap_or(false)
            });
        }

        // Apply labels filter
        if let Some(has_labels_filter) = args.has_labels {
            entities.retain(|entity| {
                let has_any_labels = entity
                    .get("labels")
                    .and_then(|labels| labels.as_array())
                    .map(|arr| !arr.is_empty())
                    .unwrap_or(false);
                has_any_labels == has_labels_filter
            });
        }

        // Apply limit
        let limit_count = args.limit.unwrap_or(100);
        entities.truncate(limit_count);

        let pretty_json = serde_json::to_string_pretty(&entities)
            .map_err(|e| McpError::internal_error(e.to_string(), None))?;
        Ok(CallToolResult::success(vec![Content::text(pretty_json)]))
    }

    async fn entity_get(&self, args: EntityGetArgs) -> Result<CallToolResult, McpError> {
        let all_entities = self.registry_ops.list("entity_registry").await?;
        let empty_vec = vec![];
        let entities = all_entities.as_array().unwrap_or(&empty_vec);

        if let Some(entity) = entities
            .iter()
            .find(|e| e.get("entity_id").and_then(|id| id.as_str()) == Some(&args.entity_id))
        {
            self.format_json_result(entity)
        } else {
            Err(McpError::invalid_params(
                format!("Entity not found: {}", args.entity_id),
                None,
            ))
        }
    }

    async fn device_get(&self, args: DeviceGetArgs) -> Result<CallToolResult, McpError> {
        let all_devices = self.registry_ops.list("device_registry").await?;
        let empty_vec = vec![];
        let devices = all_devices.as_array().unwrap_or(&empty_vec);

        if let Some(device) = devices
            .iter()
            .find(|d| d.get("id").and_then(|id| id.as_str()) == Some(&args.device_id))
        {
            self.format_json_result(device)
        } else {
            Err(McpError::invalid_params(
                format!("Device not found: {}", args.device_id),
                None,
            ))
        }
    }

    async fn entity_assign_area(
        &self,
        args: EntityAssignAreaArgs,
    ) -> Result<CallToolResult, McpError> {
        self.registry_ops
            .update(
                "entity_registry",
                &args.entity_id,
                json!({
                    "area_id": args.area_id
                }),
            )
            .await?;
        Ok(CallToolResult::success(vec![Content::text(format!(
            "Successfully assigned entity {} to area {}",
            args.entity_id, args.area_id
        ))]))
    }

    async fn entity_add_labels(
        &self,
        args: EntityAddLabelsArgs,
    ) -> Result<CallToolResult, McpError> {
        // Get current entity to preserve existing labels
        let all_entities = self.registry_ops.list("entity_registry").await?;
        let empty_vec = vec![];
        let entities = all_entities.as_array().unwrap_or(&empty_vec);

        if let Some(entity) = entities
            .iter()
            .find(|e| e.get("entity_id").and_then(|id| id.as_str()) == Some(&args.entity_id))
        {
            let mut current_labels: Vec<String> = entity
                .get("labels")
                .and_then(|l| l.as_array())
                .map(|arr| {
                    arr.iter()
                        .filter_map(|v| v.as_str().map(|s| s.into()))
                        .collect()
                })
                .unwrap_or_default();

            // Add new labels (avoid duplicates)
            for label in args.labels {
                if !current_labels.contains(&label) {
                    current_labels.push(label);
                }
            }

            self.registry_ops
                .update(
                    "entity_registry",
                    &args.entity_id,
                    json!({
                        "labels": current_labels
                    }),
                )
                .await?;

            Ok(CallToolResult::success(vec![Content::text(format!(
                "Successfully added labels to entity: {}",
                args.entity_id
            ))]))
        } else {
            Err(McpError::invalid_params(
                format!("Entity not found: {}", args.entity_id),
                None,
            ))
        }
    }

    async fn entity_remove_labels(
        &self,
        args: EntityRemoveLabelsArgs,
    ) -> Result<CallToolResult, McpError> {
        let new_labels = match args.labels {
            Some(to_remove) => {
                // Get current entity labels and remove specified ones
                let all_entities = self.registry_ops.list("entity_registry").await?;
                let empty_vec = vec![];
                let entities = all_entities.as_array().unwrap_or(&empty_vec);

                if let Some(entity) = entities.iter().find(|e| {
                    e.get("entity_id").and_then(|id| id.as_str()) == Some(&args.entity_id)
                }) {
                    let current_labels: Vec<String> = entity
                        .get("labels")
                        .and_then(|l| l.as_array())
                        .map(|arr| {
                            arr.iter()
                                .filter_map(|v| v.as_str().map(|s| s.into()))
                                .collect()
                        })
                        .unwrap_or_default();

                    current_labels
                        .into_iter()
                        .filter(|label| !to_remove.contains(label))
                        .collect()
                } else {
                    return Err(McpError::invalid_params(
                        format!("Entity not found: {}", args.entity_id),
                        None,
                    ));
                }
            }
            None => vec![], // Remove all labels
        };

        self.registry_ops
            .update(
                "entity_registry",
                &args.entity_id,
                json!({
                    "labels": new_labels
                }),
            )
            .await?;

        Ok(CallToolResult::success(vec![Content::text(format!(
            "Successfully removed labels from entity: {}",
            args.entity_id
        ))]))
    }

    async fn device_get_entities(
        &self,
        args: DeviceGetEntitiesArgs,
    ) -> Result<CallToolResult, McpError> {
        let all_entities = self.registry_ops.list("entity_registry").await?;
        let empty_vec = vec![];
        let entities = all_entities.as_array().unwrap_or(&empty_vec);

        let device_entities: Vec<&serde_json::Value> = entities
            .iter()
            .filter(|entity| {
                entity
                    .get("device_id")
                    .and_then(|id| id.as_str())
                    .map(|id| id == args.device_id)
                    .unwrap_or(false)
            })
            .collect();

        let pretty_json = serde_json::to_string_pretty(&device_entities)
            .map_err(|e| McpError::internal_error(e.to_string(), None))?;
        Ok(CallToolResult::success(vec![Content::text(pretty_json)]))
    }

    #[allow(dead_code)]
    async fn device_label_all_entities(
        &self,
        device_id: String,
        labels: Vec<String>,
    ) -> Result<CallToolResult, McpError> {
        let all_entities = self.registry_ops.list("entity_registry").await?;
        let empty_vec = vec![];
        let entities = all_entities.as_array().unwrap_or(&empty_vec);

        let device_entities: Vec<&serde_json::Value> = entities
            .iter()
            .filter(|entity| {
                entity
                    .get("device_id")
                    .and_then(|id| id.as_str())
                    .map(|id| id == device_id)
                    .unwrap_or(false)
            })
            .collect();

        if device_entities.is_empty() {
            return Ok(CallToolResult::success(vec![Content::text(format!(
                "No entities found for device: {}",
                device_id
            ))]));
        }

        let mut updated_count = 0;
        let mut errors = Vec::new();

        for entity in device_entities {
            if let Some(entity_id) = entity.get("entity_id").and_then(|id| id.as_str()) {
                match self
                    .registry_ops
                    .update(
                        "entity_registry",
                        entity_id,
                        json!({
                            "labels": labels
                        }),
                    )
                    .await
                {
                    Ok(_) => updated_count += 1,
                    Err(e) => errors.push(format!("Failed to update {}: {}", entity_id, e)),
                }
            }
        }

        let mut result = format!(
            "Successfully labeled {} entities for device {}",
            updated_count, device_id
        );
        if !errors.is_empty() {
            result.push_str(&format!("\n\nErrors: {}", errors.join(", ")));
        }

        Ok(CallToolResult::success(vec![Content::text(result)]))
    }

    async fn update_area(
        &self,
        area_id: String,
        name: Option<String>,
        floor_id: Option<String>,
        icon: Option<String>,
    ) -> Result<CallToolResult, McpError> {
        self.registry_ops
            .update(
                "area_registry",
                &area_id,
                json!({
                    "name": name,
                    "floor_id": floor_id,
                    "icon": icon
                }),
            )
            .await?;
        Ok(CallToolResult::success(vec![Content::text(format!(
            "Successfully updated area with ID: {}",
            area_id
        ))]))
    }

    async fn update_device(
        &self,
        device_id: String,
        name: Option<String>,
        area_id: Option<String>,
    ) -> Result<CallToolResult, McpError> {
        self.registry_ops
            .update(
                "device_registry",
                &device_id,
                json!({
                    "name": name,
                    "area_id": area_id
                }),
            )
            .await?;
        Ok(CallToolResult::success(vec![Content::text(format!(
            "Successfully updated device with ID: {}",
            device_id
        ))]))
    }

    async fn update_entity(
        &self,
        entity_id: String,
        name: Option<String>,
        area_id: Option<String>,
        disabled_by: Option<String>,
        labels: Option<Vec<String>>,
    ) -> Result<CallToolResult, McpError> {
        self.registry_ops
            .update(
                "entity_registry",
                &entity_id,
                json!({
                    "name": name,
                    "area_id": area_id,
                    "disabled_by": disabled_by,
                    "labels": labels
                }),
            )
            .await?;
        Ok(CallToolResult::success(vec![Content::text(format!(
            "Successfully updated entity with ID: {}",
            entity_id
        ))]))
    }

    // ============================================================================
    // CREATE TOOLS - For auto-configuration
    // ============================================================================

    async fn create_area(
        &self,
        name: String,
        floor_id: Option<String>,
        icon: Option<String>,
        aliases: Option<Vec<String>>,
    ) -> Result<CallToolResult, McpError> {
        let result = self
            .registry_ops
            .create(
                "area_registry",
                json!({
                    "name": name,
                    "floor_id": floor_id,
                    "icon": icon,
                    "aliases": aliases
                }),
            )
            .await?;

        let area_id = result
            .get("id")
            .or_else(|| result.get("area_id"))
            .and_then(|v| v.as_str())
            .unwrap_or("unknown");

        Ok(CallToolResult::success(vec![Content::text(format!(
            "Successfully created area '{}' with ID: {}",
            name, area_id
        ))]))
    }

    async fn create_floor(
        &self,
        name: String,
        level: i32,
        icon: Option<String>,
        aliases: Option<Vec<String>>,
    ) -> Result<CallToolResult, McpError> {
        let result = self
            .registry_ops
            .create(
                "floor_registry",
                json!({
                    "name": name,
                    "level": level,
                    "icon": icon,
                    "aliases": aliases
                }),
            )
            .await?;

        let floor_id = result
            .get("floor_id")
            .or_else(|| result.get("id"))
            .and_then(|v| v.as_str())
            .unwrap_or("unknown");

        Ok(CallToolResult::success(vec![Content::text(format!(
            "Successfully created floor '{}' (level {}) with ID: {}",
            name, level, floor_id
        ))]))
    }

    async fn create_label(&self, args: CreateLabelArgs) -> Result<CallToolResult, McpError> {
        let result = self
            .registry_ops
            .create(
                "label_registry",
                json!({
                    "name": args.name,
                    "icon": args.icon,
                    "color": args.color,
                    "description": args.description
                }),
            )
            .await?;

        let label_id = result
            .get("label_id")
            .or_else(|| result.get("id"))
            .and_then(|v| v.as_str())
            .unwrap_or("unknown");

        Ok(CallToolResult::success(vec![Content::text(format!(
            "Successfully created label '{}' with ID: {}",
            args.name, label_id
        ))]))
    }

    // ============================================================================
    // DELETE TOOLS - For cleanup operations
    // ============================================================================

    async fn delete_area(&self, area_id: String) -> Result<CallToolResult, McpError> {
        self.registry_ops.delete("area_registry", &area_id).await?;
        Ok(CallToolResult::success(vec![Content::text(format!(
            "Successfully deleted area with ID: {}",
            area_id
        ))]))
    }

    async fn delete_floor(&self, floor_id: String) -> Result<CallToolResult, McpError> {
        self.registry_ops
            .delete("floor_registry", &floor_id)
            .await?;
        Ok(CallToolResult::success(vec![Content::text(format!(
            "Successfully deleted floor with ID: {}",
            floor_id
        ))]))
    }

    async fn delete_label(&self, label_id: String) -> Result<CallToolResult, McpError> {
        self.registry_ops
            .delete("label_registry", &label_id)
            .await?;
        Ok(CallToolResult::success(vec![Content::text(format!(
            "Successfully deleted label with ID: {}",
            label_id
        ))]))
    }

    async fn update_label(&self, args: UpdateLabelArgs) -> Result<CallToolResult, McpError> {
        self.registry_ops
            .update(
                "label_registry",
                &args.label_id,
                json!({
                    "name": args.name,
                    "icon": args.icon,
                    "color": args.color,
                    "description": args.description
                }),
            )
            .await?;
        Ok(CallToolResult::success(vec![Content::text(format!(
            "Successfully updated label with ID: {}",
            args.label_id
        ))]))
    }

    // ============================================================================
    // ENTITY ASSIGNMENT TOOLS - For organizing devices
    // ============================================================================

    async fn assign_entity_to_area(
        &self,
        entity_id: String,
        area_id: String,
    ) -> Result<CallToolResult, McpError> {
        self.registry_ops
            .update(
                "entity_registry",
                &entity_id,
                json!({
                    "area_id": area_id
                }),
            )
            .await?;
        Ok(CallToolResult::success(vec![Content::text(format!(
            "Successfully assigned entity '{}' to area '{}'",
            entity_id, area_id
        ))]))
    }

    async fn unassign_entity_from_area(
        &self,
        entity_id: String,
    ) -> Result<CallToolResult, McpError> {
        self.registry_ops
            .update(
                "entity_registry",
                &entity_id,
                json!({
                    "area_id": null
                }),
            )
            .await?;
        Ok(CallToolResult::success(vec![Content::text(format!(
            "Successfully removed entity '{}' from its area",
            entity_id
        ))]))
    }

    async fn assign_device_to_area(
        &self,
        device_id: String,
        area_id: String,
    ) -> Result<CallToolResult, McpError> {
        self.registry_ops
            .update(
                "device_registry",
                &device_id,
                json!({
                    "area_id": area_id
                }),
            )
            .await?;
        Ok(CallToolResult::success(vec![Content::text(format!(
            "Successfully assigned device '{}' to area '{}'",
            device_id, area_id
        ))]))
    }

    async fn add_labels_to_entity(
        &self,
        entity_id: String,
        labels: Vec<String>,
    ) -> Result<CallToolResult, McpError> {
        self.registry_ops
            .update(
                "entity_registry",
                &entity_id,
                json!({
                    "labels": labels
                }),
            )
            .await?;
        Ok(CallToolResult::success(vec![Content::text(format!(
            "Successfully added labels {:?} to entity '{}'",
            labels, entity_id
        ))]))
    }

    async fn clear_entity_labels(&self, entity_id: String) -> Result<CallToolResult, McpError> {
        self.registry_ops
            .update(
                "entity_registry",
                &entity_id,
                json!({
                    "labels": []
                }),
            )
            .await?;
        Ok(CallToolResult::success(vec![Content::text(format!(
            "Successfully cleared all labels from entity '{}'",
            entity_id
        ))]))
    }

    async fn label_device(
        &self,
        device_id: String,
        labels: Vec<String>,
    ) -> Result<CallToolResult, McpError> {
        // First validate the device exists
        let all_devices = self.registry_ops.list("device_registry").await?;
        let empty_vec = vec![];
        let device_exists = all_devices
            .as_array()
            .unwrap_or(&empty_vec)
            .iter()
            .any(|d| d.get("id").and_then(|v| v.as_str()) == Some(&device_id));

        if !device_exists {
            return Ok(CallToolResult::success(vec![Content::text(format!(
                "❌ Device '{}' not found. Use validate_device_id() to check if the device exists.",
                device_id
            ))]));
        }

        // Get all entities for this device
        let all_entities = self.registry_ops.list("entity_registry").await?;
        let device_entities: Vec<String> = all_entities
            .as_array()
            .unwrap_or(&empty_vec)
            .iter()
            .filter_map(|entity| {
                if entity.get("device_id").and_then(|v| v.as_str()) == Some(&device_id) {
                    entity
                        .get("entity_id")
                        .and_then(|v| v.as_str())
                        .map(|s| s.into())
                } else {
                    None
                }
            })
            .collect();

        if device_entities.is_empty() {
            return Ok(CallToolResult::success(vec![Content::text(format!(
                "⚠️ Device '{}' exists but has no entities to label.",
                device_id
            ))]));
        }

        // Apply labels to all entities
        let mut success_count = 0;
        let mut errors = Vec::new();

        for entity_id in &device_entities {
            match self
                .registry_ops
                .update(
                    "entity_registry",
                    entity_id,
                    json!({
                        "labels": labels
                    }),
                )
                .await
            {
                Ok(_) => success_count += 1,
                Err(e) => errors.push(format!("Failed to label {}: {}", entity_id, e)),
            }
        }

        let message = if errors.is_empty() {
            format!(
                "✅ Successfully labeled device '{}' by applying labels {:?} to {} entities:\n{}",
                device_id,
                labels,
                success_count,
                device_entities
                    .iter()
                    .map(|id| format!("  - {}", id))
                    .collect::<Vec<_>>()
                    .join("\n")
            )
        } else {
            format!(
                "⚠️ Partially labeled device '{}': {}/{} entities labeled successfully.\n\nSuccess:\n{}\n\nErrors:\n{}",
                device_id,
                success_count,
                device_entities.len(),
                device_entities
                    .iter()
                    .take(success_count)
                    .map(|id| format!("  - {}", id))
                    .collect::<Vec<_>>()
                    .join("\n"),
                errors.join("\n")
            )
        };

        Ok(CallToolResult::success(vec![Content::text(message)]))
    }

    async fn create_and_label_device(
        &self,
        args: CreateAndLabelDeviceArgs,
    ) -> Result<CallToolResult, McpError> {
        // First create the label
        let result = self
            .registry_ops
            .create(
                "label_registry",
                json!({
                    "name": args.label_name,
                    "icon": args.icon,
                    "color": args.color,
                    "description": args.description
                }),
            )
            .await?;

        let label_id = result
            .get("label_id")
            .or_else(|| result.get("id"))
            .and_then(|v| v.as_str())
            .unwrap_or(&args.label_name); // Fallback to name if ID not found

        // Then apply to device (all its entities)
        match self
            .label_device(args.device_id.clone(), vec![label_id.into()])
            .await
        {
            Ok(device_result) => {
                let device_message = device_result
                    .content
                    .first()
                    .and_then(|c| match &c.raw {
                        RawContent::Text(text) => Some(text.text.as_str()),
                        _ => None,
                    })
                    .unwrap_or("Device labeling completed");

                Ok(CallToolResult::success(vec![Content::text(format!(
                    "✅ Created label '{}' (ID: {}) and applied to device '{}'.\n\n{}",
                    args.label_name, label_id, args.device_id, device_message
                ))]))
            }
            Err(e) => Ok(CallToolResult::success(vec![Content::text(format!(
                "⚠️ Created label '{}' successfully, but failed to apply to device '{}': {}",
                args.label_name, args.device_id, e
            ))])),
        }
    }

    async fn create_and_assign_label(
        &self,
        args: CreateAndAssignLabelArgs,
    ) -> Result<CallToolResult, McpError> {
        // First create the label
        let result = self
            .registry_ops
            .create(
                "label_registry",
                json!({
                    "name": args.name,
                    "icon": args.icon,
                    "color": args.color,
                    "description": args.description
                }),
            )
            .await?;

        let label_id = result
            .get("label_id")
            .or_else(|| result.get("id"))
            .and_then(|v| v.as_str())
            .unwrap_or(&args.name); // Fallback to name if ID not found

        // Then assign to all entities
        let mut success_count = 0;
        let mut errors = Vec::new();

        for entity_id in &args.entity_ids {
            match self
                .registry_ops
                .update(
                    "entity_registry",
                    entity_id,
                    json!({
                        "labels": [label_id]
                    }),
                )
                .await
            {
                Ok(_) => success_count += 1,
                Err(e) => errors.push(format!("Failed to assign to {}: {}", entity_id, e)),
            }
        }

        let message = if errors.is_empty() {
            format!(
                "Successfully created label '{}' and assigned to {} entities",
                args.name, success_count
            )
        } else {
            format!(
                "Created label '{}', assigned to {}/{} entities. Errors: {}",
                args.name,
                success_count,
                args.entity_ids.len(),
                errors.join(", ")
            )
        };

        Ok(CallToolResult::success(vec![Content::text(message)]))
    }

    // ============================================================================
    // SUMMARY TOOLS - For efficient overview without token overflow
    // ============================================================================

    async fn get_summary(&self) -> Result<CallToolResult, McpError> {
        // Get counts for each registry
        let areas = self.registry_ops.list("area_registry").await?;
        let devices = self.registry_ops.list("device_registry").await?;
        let entities = self.registry_ops.list("entity_registry").await?;
        let floors = self.registry_ops.list("floor_registry").await?;
        let labels = self.registry_ops.list("label_registry").await?;

        let empty_vec = vec![];
        let areas_count = areas.as_array().unwrap_or(&empty_vec).len();
        let devices_count = devices.as_array().unwrap_or(&empty_vec).len();
        let entities_count = entities.as_array().unwrap_or(&empty_vec).len();
        let floors_count = floors.as_array().unwrap_or(&empty_vec).len();
        let labels_count = labels.as_array().unwrap_or(&empty_vec).len();

        // Count unassigned entities
        let unassigned_entities = entities
            .as_array()
            .unwrap_or(&empty_vec)
            .iter()
            .filter(|entity| {
                entity.get("area_id").is_none()
                    || entity
                        .get("area_id")
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .is_empty()
            })
            .count();

        // Count entities by domain
        let mut domain_counts: std::collections::HashMap<String, usize> =
            std::collections::HashMap::new();
        for entity in entities.as_array().unwrap_or(&empty_vec) {
            if let Some(entity_id) = entity.get("entity_id").and_then(|v| v.as_str()) {
                let domain: String = entity_id.split('.').next().unwrap_or("unknown").into();
                *domain_counts.entry(domain).or_insert(0) += 1;
            }
        }

        let summary = json!({
            "total_counts": {
                "areas": areas_count,
                "devices": devices_count,
                "entities": entities_count,
                "floors": floors_count,
                "labels": labels_count
            },
            "entity_analysis": {
                "unassigned_entities": unassigned_entities,
                "assigned_entities": entities_count - unassigned_entities,
                "domains": domain_counts
            },
            "organization_status": {
                "assignment_percentage": if entities_count > 0 {
                    ((entities_count - unassigned_entities) as f64 / entities_count as f64 * 100.0).round()
                } else { 0.0 },
                "areas_per_floor": if floors_count > 0 { areas_count as f64 / floors_count as f64 } else { 0.0 }
            }
        });

        let pretty_json = serde_json::to_string_pretty(&summary)
            .map_err(|e| McpError::internal_error(e.to_string(), None))?;
        Ok(CallToolResult::success(vec![Content::text(pretty_json)]))
    }

    // ============================================================================
    // BULK OPERATIONS - For efficient auto-configuration
    // ============================================================================

    async fn get_entities_in_area(&self, area_id: String) -> Result<CallToolResult, McpError> {
        let all_entities = self.registry_ops.list("entity_registry").await?;
        let empty_vec = vec![];

        let entities_in_area: Vec<&serde_json::Value> = all_entities
            .as_array()
            .unwrap_or(&empty_vec)
            .iter()
            .filter(|entity| {
                entity
                    .get("area_id")
                    .and_then(|v| v.as_str())
                    .map(|id| id == area_id)
                    .unwrap_or(false)
            })
            .collect();

        let pretty_json = serde_json::to_string_pretty(&entities_in_area)
            .map_err(|e| McpError::internal_error(e.to_string(), None))?;
        Ok(CallToolResult::success(vec![Content::text(pretty_json)]))
    }

    async fn get_unassigned_entities(&self) -> Result<CallToolResult, McpError> {
        let all_entities = self.registry_ops.list("entity_registry").await?;
        let empty_vec = vec![];

        let unassigned_entities: Vec<&serde_json::Value> = all_entities
            .as_array()
            .unwrap_or(&empty_vec)
            .iter()
            .filter(|entity| {
                entity.get("area_id").is_none()
                    || entity
                        .get("area_id")
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .is_empty()
            })
            .collect();

        let pretty_json = serde_json::to_string_pretty(&unassigned_entities)
            .map_err(|e| McpError::internal_error(e.to_string(), None))?;
        Ok(CallToolResult::success(vec![Content::text(pretty_json)]))
    }

    async fn find_area_by_name(&self, name: String) -> Result<CallToolResult, McpError> {
        let all_areas = self.registry_ops.list("area_registry").await?;
        let name_lower = name.to_lowercase();
        let empty_vec = vec![];

        let matching_areas: Vec<&serde_json::Value> = all_areas
            .as_array()
            .unwrap_or(&empty_vec)
            .iter()
            .filter(|area| {
                area.get("name")
                    .and_then(|v| v.as_str())
                    .map(|area_name| area_name.to_lowercase().contains(&name_lower))
                    .unwrap_or(false)
            })
            .collect();

        let pretty_json = serde_json::to_string_pretty(&matching_areas)
            .map_err(|e| McpError::internal_error(e.to_string(), None))?;
        Ok(CallToolResult::success(vec![Content::text(pretty_json)]))
    }

    async fn find_floor_by_name(&self, name: String) -> Result<CallToolResult, McpError> {
        let all_floors = self.registry_ops.list("floor_registry").await?;
        let name_lower = name.to_lowercase();
        let empty_vec = vec![];

        let matching_floors: Vec<&serde_json::Value> = all_floors
            .as_array()
            .unwrap_or(&empty_vec)
            .iter()
            .filter(|floor| {
                floor
                    .get("name")
                    .and_then(|v| v.as_str())
                    .map(|floor_name| floor_name.to_lowercase().contains(&name_lower))
                    .unwrap_or(false)
            })
            .collect();

        let pretty_json = serde_json::to_string_pretty(&matching_floors)
            .map_err(|e| McpError::internal_error(e.to_string(), None))?;
        Ok(CallToolResult::success(vec![Content::text(pretty_json)]))
    }

    async fn search_entities(
        &self,
        pattern: String,
        limit: Option<usize>,
    ) -> Result<CallToolResult, McpError> {
        let all_entities = self.registry_ops.list("entity_registry").await?;
        let pattern_lower = pattern.to_lowercase();
        let empty_vec = vec![];
        let max_results = limit.unwrap_or(20); // Default to 20 results

        let matching_entities: Vec<&serde_json::Value> = all_entities
            .as_array()
            .unwrap_or(&empty_vec)
            .iter()
            .filter(|entity| {
                // Check entity_id
                let entity_id_match = entity
                    .get("entity_id")
                    .and_then(|v| v.as_str())
                    .map(|id| id.to_lowercase().contains(&pattern_lower))
                    .unwrap_or(false);

                // Check friendly name if available
                let name_match = entity
                    .get("name")
                    .and_then(|v| v.as_str())
                    .map(|name| name.to_lowercase().contains(&pattern_lower))
                    .unwrap_or(false);

                entity_id_match || name_match
            })
            .take(max_results)
            .collect();

        let pretty_json = serde_json::to_string_pretty(&matching_entities)
            .map_err(|e| McpError::internal_error(e.to_string(), None))?;
        Ok(CallToolResult::success(vec![Content::text(pretty_json)]))
    }

    async fn find_entities_by_label(
        &self,
        label_id: String,
        limit: Option<usize>,
    ) -> Result<CallToolResult, McpError> {
        let all_entities = self.registry_ops.list("entity_registry").await?;
        let empty_vec = vec![];
        let max_results = limit.unwrap_or(50); // Default to 50 results

        let matching_entities: Vec<&serde_json::Value> = all_entities
            .as_array()
            .unwrap_or(&empty_vec)
            .iter()
            .filter(|entity| {
                // Check if entity has the specified label
                entity
                    .get("labels")
                    .and_then(|labels| labels.as_array())
                    .map(|labels_array| {
                        labels_array
                            .iter()
                            .any(|label| label.as_str().map(|l| l == label_id).unwrap_or(false))
                    })
                    .unwrap_or(false)
            })
            .take(max_results)
            .collect();

        let pretty_json = serde_json::to_string_pretty(&matching_entities)
            .map_err(|e| McpError::internal_error(e.to_string(), None))?;
        Ok(CallToolResult::success(vec![Content::text(pretty_json)]))
    }

    // ============================================================================
    // DISCOVERY & INTROSPECTION TOOLS - For better discoverability
    // ============================================================================

    async fn discover_resources(&self) -> Result<CallToolResult, McpError> {
        let schemas = self.schemas.lock().await;
        let resources: Vec<String> = schemas
            .as_mapping()
            .unwrap_or(&serde_yaml::Mapping::new())
            .keys()
            .filter_map(|k| k.as_str().map(|s| s.into()))
            .collect();

        let mut output = String::from("# Available Home Assistant Resources\n\n");
        for resource in &resources {
            let context = self.get_resource_context(resource);
            output.push_str(&format!("## {}\n{}\n\n", resource, context));
        }

        output.push_str(&format!("\n**Total resources available: {}**\n\nUse `describe_resource(resource_name)` for detailed schema information.", resources.len()));

        Ok(CallToolResult::success(vec![Content::text(output)]))
    }

    async fn discover_tools(&self) -> Result<CallToolResult, McpError> {
        let tools = vec![
            (
                "describe_config",
                "Get detailed schema and context for any HA resource",
            ),
            (
                "list_all_resources",
                "Show all available HA resources with descriptions",
            ),
            ("discover_tools", "List all MCP tools with descriptions"),
            (
                "validate_entity_id",
                "Check if an entity ID exists and get its details",
            ),
            (
                "validate_device_id",
                "Check if a device ID exists and get its details",
            ),
            (
                "get_entities_for_device",
                "Get all entities belonging to a device",
            ),
            (
                "get_device_summary",
                "Show device info with all entities and their labels",
            ),
            (
                "analyze_configuration",
                "Get comprehensive analysis of your HA setup",
            ),
            (
                "get_orphaned_entities",
                "Find entities not assigned to any area",
            ),
            (
                "recommend_labeling",
                "Find unlabeled entities and suggest labeling with entity IDs",
            ),
            // Listing tools
            ("list_areas", "List all areas"),
            ("list_devices", "List devices with optional filtering"),
            ("list_entities", "List entities with optional filtering"),
            ("list_floors", "List all floors"),
            ("list_labels", "List all labels"),
            ("list_sensors", "List sensor states"),
            ("get_summary", "Get configuration summary statistics"),
            // Creation tools
            ("create_area", "Create a new area"),
            ("create_floor", "Create a new floor"),
            (
                "create_label",
                "Create a new label with color and icon support",
            ),
            (
                "update_label",
                "Update label properties (name, icon, color, description)",
            ),
            (
                "create_and_assign_label",
                "Create label and immediately assign to entities",
            ),
            // Update tools
            ("update_area", "Update area properties (name, floor, icon)"),
            ("update_device", "Update device properties (name, area)"),
            (
                "update_entity",
                "Update entity properties (name, area, labels)",
            ),
            // Assignment tools
            ("assign_entity_to_area", "Assign an entity to an area"),
            ("assign_device_to_area", "Assign a device to an area"),
            ("add_labels_to_entity", "Add labels to an entity"),
            ("clear_entity_labels", "Remove all labels from an entity"),
            (
                "label_device",
                "Label a device by applying labels to ALL its entities",
            ),
            (
                "create_and_label_device",
                "Create label and apply to all entities of a device",
            ),
            // Search/discovery tools
            ("find_area_by_name", "Search areas by name"),
            ("find_floor_by_name", "Search floors by name"),
            ("search_entities", "Search entities by name/ID pattern"),
            (
                "find_entities_by_label",
                "Find entities with specific label",
            ),
            ("get_entities_in_area", "Get all entities in an area"),
            (
                "get_unassigned_entities",
                "Get entities not assigned to any area",
            ),
            // Deletion tools
            ("delete_area", "Delete an area"),
            ("delete_floor", "Delete a floor"),
            ("delete_label", "Delete a label"),
        ];

        let mut output = String::from("# Available MCP Tools\n\n");

        // Group tools by category
        let categories = vec![
            (
                "Discovery & Introspection",
                vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
            ),
            ("Listing & Summary", vec![10, 11, 12, 13, 14, 15, 16]),
            ("Creation", vec![17, 18, 19, 20]),
            ("Updates", vec![21, 22, 23]),
            ("Assignment & Organization", vec![24, 25, 26, 27, 28, 29]),
            ("Search & Discovery", vec![30, 31, 32, 33, 34, 35]),
            ("Deletion", vec![36, 37, 38]),
        ];

        for (category, indices) in categories {
            output.push_str(&format!("## {}\n", category));
            for &i in &indices {
                if i < tools.len() {
                    let (name, desc) = &tools[i];
                    output.push_str(&format!("- **{}**: {}\n", name, desc));
                }
            }
            output.push('\n');
        }

        output.push_str("**Usage**: Call any tool with appropriate parameters. Use `describe_config()` for resource schemas.\n");

        Ok(CallToolResult::success(vec![Content::text(output)]))
    }

    async fn validate_entity_id(&self, entity_id: String) -> Result<CallToolResult, McpError> {
        // Check if it looks like a valid entity ID format
        if !entity_id.contains('.') {
            return Ok(CallToolResult::success(vec![Content::text(format!(
                "❌ Invalid entity ID format: '{}'\n\nEntity IDs should be in format 'domain.name' (e.g., 'sensor.temperature', 'light.kitchen')\n\nDid you mean to use validate_device_id() instead?",
                entity_id
            ))]));
        }

        let all_entities = self.registry_ops.list("entity_registry").await?;
        let empty_vec = vec![];

        if let Some(entity) = all_entities
            .as_array()
            .unwrap_or(&empty_vec)
            .iter()
            .find(|e| e.get("entity_id").and_then(|v| v.as_str()) == Some(&entity_id))
        {
            let pretty_json = serde_json::to_string_pretty(entity)
                .map_err(|e| McpError::internal_error(e.to_string(), None))?;
            Ok(CallToolResult::success(vec![Content::text(format!(
                "✅ Entity '{}' exists\n\n```json\n{}\n```",
                entity_id, pretty_json
            ))]))
        } else {
            Ok(CallToolResult::success(vec![Content::text(format!(
                "❌ Entity '{}' not found in entity registry\n\nUse search_entities() to find similar entities.",
                entity_id
            ))]))
        }
    }

    async fn validate_device_id(&self, device_id: String) -> Result<CallToolResult, McpError> {
        let all_devices = self.registry_ops.list("device_registry").await?;
        let empty_vec = vec![];

        if let Some(device) = all_devices
            .as_array()
            .unwrap_or(&empty_vec)
            .iter()
            .find(|d| d.get("id").and_then(|v| v.as_str()) == Some(&device_id))
        {
            let pretty_json = serde_json::to_string_pretty(device)
                .map_err(|e| McpError::internal_error(e.to_string(), None))?;
            Ok(CallToolResult::success(vec![Content::text(format!(
                "✅ Device '{}' exists\n\n```json\n{}\n```",
                device_id, pretty_json
            ))]))
        } else {
            Ok(CallToolResult::success(vec![Content::text(format!(
                "❌ Device '{}' not found in device registry\n\nUse list_devices() to see available devices.",
                device_id
            ))]))
        }
    }

    async fn get_entities_for_device(&self, device_id: String) -> Result<CallToolResult, McpError> {
        let all_entities = self.registry_ops.list("entity_registry").await?;
        let empty_vec = vec![];

        let device_entities: Vec<&serde_json::Value> = all_entities
            .as_array()
            .unwrap_or(&empty_vec)
            .iter()
            .filter(|entity| {
                entity
                    .get("device_id")
                    .and_then(|id| id.as_str())
                    .map(|id| id == device_id)
                    .unwrap_or(false)
            })
            .collect();

        if device_entities.is_empty() {
            Ok(CallToolResult::success(vec![Content::text(format!(
                "No entities found for device '{}'\n\nUse validate_device_id() to check if the device exists.",
                device_id
            ))]))
        } else {
            let pretty_json = serde_json::to_string_pretty(&device_entities)
                .map_err(|e| McpError::internal_error(e.to_string(), None))?;
            Ok(CallToolResult::success(vec![Content::text(format!(
                "Found {} entities for device '{}'\n\n```json\n{}\n```",
                device_entities.len(),
                device_id,
                pretty_json
            ))]))
        }
    }

    async fn get_device_summary(&self, device_id: String) -> Result<CallToolResult, McpError> {
        // Get device info
        let all_devices = self.registry_ops.list("device_registry").await?;
        let empty_vec = vec![];

        let device_info = all_devices
            .as_array()
            .unwrap_or(&empty_vec)
            .iter()
            .find(|d| d.get("id").and_then(|v| v.as_str()) == Some(&device_id));

        if device_info.is_none() {
            return Ok(CallToolResult::success(vec![Content::text(format!(
                "❌ Device '{}' not found. Use validate_device_id() to check if the device exists.",
                device_id
            ))]));
        }

        let device = device_info.unwrap();
        let device_name = device
            .get("name_by_user")
            .or_else(|| device.get("name"))
            .and_then(|v| v.as_str())
            .unwrap_or("Unknown Device");
        let manufacturer = device
            .get("manufacturer")
            .and_then(|v| v.as_str())
            .unwrap_or("Unknown");
        let model = device
            .get("model")
            .and_then(|v| v.as_str())
            .unwrap_or("Unknown");

        // Get all entities for this device
        let all_entities = self.registry_ops.list("entity_registry").await?;
        let device_entities: Vec<&serde_json::Value> = all_entities
            .as_array()
            .unwrap_or(&empty_vec)
            .iter()
            .filter(|entity| {
                entity
                    .get("device_id")
                    .and_then(|id| id.as_str())
                    .map(|id| id == device_id)
                    .unwrap_or(false)
            })
            .collect();

        let mut output = format!(
            "# Device Summary: {}\n\n\
            - **Device ID**: `{}`\n\
            - **Manufacturer**: {}\n\
            - **Model**: {}\n\
            - **Total Entities**: {}\n\n",
            device_name,
            device_id,
            manufacturer,
            model,
            device_entities.len()
        );

        if device_entities.is_empty() {
            output.push_str("⚠️ This device has no entities.\n");
        } else {
            output.push_str("## Entities and Labels\n\n");

            // Group entities by domain
            let mut domain_groups: std::collections::HashMap<String, Vec<&serde_json::Value>> =
                std::collections::HashMap::new();

            for entity in &device_entities {
                if let Some(entity_id) = entity.get("entity_id").and_then(|v| v.as_str()) {
                    let domain: String = entity_id.split('.').next().unwrap_or("unknown").into();
                    domain_groups.entry(domain).or_default().push(*entity);
                }
            }

            for (domain, entities) in domain_groups {
                output.push_str(&format!("### {} Domain\n", domain));

                for entity in entities {
                    let entity_id = entity
                        .get("entity_id")
                        .and_then(|v| v.as_str())
                        .unwrap_or("unknown");
                    let entity_name = entity.get("name").and_then(|v| v.as_str()).unwrap_or("");

                    let labels = entity
                        .get("labels")
                        .and_then(|labels| labels.as_array())
                        .map(|arr| {
                            if arr.is_empty() {
                                "No labels".into()
                            } else {
                                arr.iter()
                                    .filter_map(|l| l.as_str())
                                    .map(|l| format!("`{}`", l))
                                    .collect::<Vec<_>>()
                                    .join(", ")
                            }
                        })
                        .unwrap_or_else(|| "No labels".into());

                    if entity_name.is_empty() {
                        output.push_str(&format!("- **{}**: {}\n", entity_id, labels));
                    } else {
                        output.push_str(&format!(
                            "- **{}** ({}): {}\n",
                            entity_id, entity_name, labels
                        ));
                    }
                }
                output.push('\n');
            }

            // Add helpful note
            output.push_str("💡 **Note**: Labels are only visible on individual entity pages in Home Assistant, not on the device page. This summary shows you what labels are applied to each entity belonging to this device.\n");
        }

        Ok(CallToolResult::success(vec![Content::text(output)]))
    }

    async fn analyze_configuration(&self) -> Result<CallToolResult, McpError> {
        // Get all registries
        let areas = self.registry_ops.list("area_registry").await?;
        let devices = self.registry_ops.list("device_registry").await?;
        let entities = self.registry_ops.list("entity_registry").await?;
        let floors = self.registry_ops.list("floor_registry").await?;
        let labels = self.registry_ops.list("label_registry").await?;

        let empty_vec = vec![];
        let areas_array = areas.as_array().unwrap_or(&empty_vec);
        let devices_array = devices.as_array().unwrap_or(&empty_vec);
        let entities_array = entities.as_array().unwrap_or(&empty_vec);
        let floors_array = floors.as_array().unwrap_or(&empty_vec);
        let labels_array = labels.as_array().unwrap_or(&empty_vec);

        // Analyze unassigned items
        let unassigned_entities = entities_array
            .iter()
            .filter(|e| {
                e.get("area_id").is_none()
                    || e.get("area_id")
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .is_empty()
            })
            .count();

        let unassigned_devices = devices_array
            .iter()
            .filter(|d| {
                d.get("area_id").is_none()
                    || d.get("area_id")
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .is_empty()
            })
            .count();

        let areas_without_floor = areas_array
            .iter()
            .filter(|a| {
                a.get("floor_id").is_none()
                    || a.get("floor_id")
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .is_empty()
            })
            .count();

        // Analyze entity domains
        let mut domain_counts: std::collections::HashMap<String, usize> =
            std::collections::HashMap::new();
        for entity in entities_array {
            if let Some(entity_id) = entity.get("entity_id").and_then(|v| v.as_str()) {
                let domain: String = entity_id.split('.').next().unwrap_or("unknown").into();
                *domain_counts.entry(domain).or_insert(0) += 1;
            }
        }

        let mut top_domains: Vec<_> = domain_counts.iter().collect();
        top_domains.sort_by(|a, b| b.1.cmp(a.1));

        let analysis = format!(
            "# Home Assistant Configuration Analysis\n\n\
            ## Overview\n\
            - **Total Areas**: {}\n\
            - **Total Devices**: {}\n\
            - **Total Entities**: {}\n\
            - **Total Floors**: {}\n\
            - **Total Labels**: {}\n\n\
            ## Organization Status\n\
            - **Unassigned Entities**: {} ({:.1}%)\n\
            - **Unassigned Devices**: {} ({:.1}%)\n\
            - **Areas without Floor**: {} ({:.1}%)\n\n\
            ## Top Entity Domains\n{}\n\n\
            ## Recommendations\n{}\n",
            areas_array.len(),
            devices_array.len(),
            entities_array.len(),
            floors_array.len(),
            labels_array.len(),
            unassigned_entities,
            if !entities_array.is_empty() {
                unassigned_entities as f64 / entities_array.len() as f64 * 100.0
            } else {
                0.0
            },
            unassigned_devices,
            if !devices_array.is_empty() {
                unassigned_devices as f64 / devices_array.len() as f64 * 100.0
            } else {
                0.0
            },
            areas_without_floor,
            if !areas_array.is_empty() {
                areas_without_floor as f64 / areas_array.len() as f64 * 100.0
            } else {
                0.0
            },
            top_domains
                .iter()
                .take(10)
                .map(|(domain, count)| format!("- **{}**: {}", domain, count))
                .collect::<Vec<_>>()
                .join("\n"),
            self.generate_recommendations(
                unassigned_entities,
                unassigned_devices,
                areas_without_floor,
                floors_array.len()
            )
        );

        Ok(CallToolResult::success(vec![Content::text(analysis)]))
    }

    async fn find_orphaned_entities(
        &self,
        limit: Option<usize>,
    ) -> Result<CallToolResult, McpError> {
        let all_entities = self.registry_ops.list("entity_registry").await?;
        let empty_vec = vec![];
        let max_results = limit.unwrap_or(100);

        let orphaned_entities: Vec<&serde_json::Value> = all_entities
            .as_array()
            .unwrap_or(&empty_vec)
            .iter()
            .filter(|entity| {
                entity.get("area_id").is_none()
                    || entity
                        .get("area_id")
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .is_empty()
            })
            .take(max_results)
            .collect();

        let summary = format!(
            "# Orphaned Entities (Not Assigned to Areas)\n\n\
            Found {} orphaned entities (showing first {})\n\n\
            💡 **Tip**: Use `entity_assign_area()` to organize these entities.\n\n\
            ```json\n{}\n```",
            orphaned_entities.len(),
            max_results.min(orphaned_entities.len()),
            serde_json::to_string_pretty(&orphaned_entities)
                .map_err(|e| McpError::internal_error(e.to_string(), None))?
        );

        Ok(CallToolResult::success(vec![Content::text(summary)]))
    }

    async fn recommend_labels(&self, limit: Option<usize>) -> Result<CallToolResult, McpError> {
        let all_entities = self.registry_ops.list("entity_registry").await?;
        let all_devices = self.registry_ops.list("device_registry").await?;
        let all_labels = self.registry_ops.list("label_registry").await?;
        let empty_vec = vec![];
        let max_results = limit.unwrap_or(50);

        // Find entities without labels
        let unlabeled_entities: Vec<&serde_json::Value> = all_entities
            .as_array()
            .unwrap_or(&empty_vec)
            .iter()
            .filter(|entity| {
                entity
                    .get("labels")
                    .and_then(|labels| labels.as_array())
                    .map(|arr| arr.is_empty())
                    .unwrap_or(true)
            })
            .take(max_results)
            .collect();

        // Get existing labels for suggestions
        let existing_labels: Vec<String> = all_labels
            .as_array()
            .unwrap_or(&empty_vec)
            .iter()
            .filter_map(|label| {
                label
                    .get("label_id")
                    .or_else(|| label.get("name"))
                    .and_then(|v| v.as_str())
                    .map(|s| s.into())
            })
            .collect();

        // Create device lookup for entity grouping
        let devices_map: std::collections::HashMap<String, &serde_json::Value> = all_devices
            .as_array()
            .unwrap_or(&empty_vec)
            .iter()
            .filter_map(|device| {
                device
                    .get("id")
                    .and_then(|id| id.as_str())
                    .map(|id| (id.into(), device))
            })
            .collect();

        // Analyze domains and group by devices
        type DomainAnalysis =
            std::collections::HashMap<String, Vec<(String, Option<String>, Option<String>)>>;
        let mut domain_analysis: DomainAnalysis = std::collections::HashMap::new();

        for entity in &unlabeled_entities {
            if let Some(entity_id) = entity.get("entity_id").and_then(|v| v.as_str()) {
                let domain: String = entity_id.split('.').next().unwrap_or("unknown").into();

                // Get device info if available
                let device_id = entity.get("device_id").and_then(|v| v.as_str());
                let device_name = device_id
                    .and_then(|id| devices_map.get(id))
                    .and_then(|device| {
                        device
                            .get("name_by_user")
                            .or_else(|| device.get("name"))
                            .and_then(|v| v.as_str())
                    });

                domain_analysis.entry(domain.clone()).or_default().push((
                    entity_id.into(),
                    device_id.map(|s| s.into()),
                    device_name.map(|s| s.into()),
                ));
            }
        }

        let mut output = format!(
            "# Labeling Recommendations\n\n\
            Found {} unlabeled entities (showing first {})\n\n\
            ## Suggested Labels by Domain\n\n",
            unlabeled_entities.len(),
            max_results.min(unlabeled_entities.len())
        );

        // Generate domain-specific recommendations with device context
        for (domain, entity_data) in &domain_analysis {
            let suggested_labels = self.suggest_labels_for_domain(domain);
            let entity_ids: Vec<String> = entity_data.iter().map(|(id, _, _)| id.clone()).collect();

            output.push_str(&format!(
                "### {} Domain ({} entities)\n\
                **Suggested Labels**: {}\n\n",
                domain,
                entity_data.len(),
                suggested_labels.join(", ")
            ));

            // Group by device for better organization
            type DeviceGroups<'a> = std::collections::HashMap<
                String,
                Vec<&'a (String, Option<String>, Option<String>)>,
            >;
            let mut device_groups: DeviceGroups = std::collections::HashMap::new();

            for entity_info in entity_data {
                let device_key = match (&entity_info.1, &entity_info.2) {
                    (Some(device_id), Some(device_name)) => {
                        format!("{} ({})", device_name, &device_id[..8])
                    }
                    (Some(device_id), None) => format!("Device {}", &device_id[..8]),
                    _ => "No Device".into(),
                };
                device_groups
                    .entry(device_key)
                    .or_default()
                    .push(entity_info);
            }

            output.push_str("**Entities by Device**:\n");
            for (device_key, entities) in &device_groups {
                output.push_str(&format!("- **{}**:\n", device_key));
                for (entity_id, device_id, _) in entities {
                    output.push_str(&format!("  - `{}` ", entity_id));
                    if let Some(dev_id) = device_id {
                        output.push_str(&format!("(device: `{}`)", dev_id));
                    }
                    output.push('\n');
                }
            }

            output.push_str(&format!(
                "\n**Quick Commands**:\n{}\n\n",
                self.generate_labeling_commands(&entity_ids, &suggested_labels)
            ));
        }

        // Add device-specific recommendations
        output.push_str(
            &self
                .generate_device_labeling_suggestions(&devices_map, &all_entities)
                .await?,
        );

        // Add existing labels section
        if !existing_labels.is_empty() {
            output.push_str(&format!(
                "## Existing Labels\n\
                You can also use these existing labels:\n{}\n\n",
                existing_labels
                    .iter()
                    .map(|l| format!("- `{}`", l))
                    .collect::<Vec<_>>()
                    .join("\n")
            ));
        }

        output.push_str(
            "## Usage Examples\n\
            ```\n\
            # Create and assign a new label to entities from same device\n\
            create_and_assign_label(name=\"security\", entity_ids=[\"binary_sensor.door\", \"binary_sensor.motion\"])\n\n\
            # Add existing label to entity\n\
            add_labels_to_entity(entity_id=\"light.kitchen\", labels=[\"lighting\"])\n\n\
            # Label ALL entities from a specific device (this is what you want for \"device labeling\"!)\n\
            label_device(device_id=\"abc123\", labels=[\"security\"])\n\n\
            # Create new label and apply to entire device\n\
            create_and_label_device(label_name=\"kitchen_devices\", device_id=\"abc123\")\n\
            ```\n\n\
            💡 **Note**: In Home Assistant, labels can only be applied to entities, not devices directly. \
            The `label_device()` and `create_and_label_device()` tools work by applying labels to ALL entities \
            that belong to the specified device.\n"
        );

        Ok(CallToolResult::success(vec![Content::text(output)]))
    }

    /// Suggest appropriate labels for a domain
    fn suggest_labels_for_domain(&self, domain: &str) -> Vec<String> {
        match domain {
            "light" => vec!["lighting".into(), "ambiance".into()],
            "sensor" => vec!["monitoring".into(), "data".into(), "environmental".into()],
            "binary_sensor" => vec!["security".into(), "monitoring".into(), "safety".into()],
            "switch" => vec!["control".into(), "automation".into()],
            "climate" => vec!["hvac".into(), "comfort".into(), "energy".into()],
            "cover" => vec!["window_treatment".into(), "privacy".into()],
            "lock" => vec!["security".into(), "access".into()],
            "camera" => vec!["security".into(), "surveillance".into()],
            "media_player" => vec!["entertainment".into(), "audio_video".into()],
            "alarm_control_panel" => vec!["security".into(), "alarm".into()],
            "device_tracker" => vec!["presence".into(), "location".into()],
            "vacuum" => vec!["cleaning".into(), "automation".into()],
            "fan" => vec!["comfort".into(), "climate".into()],
            "water_heater" => vec!["energy".into(), "utilities".into()],
            "person" => vec!["presence".into(), "family".into()],
            _ => vec!["uncategorized".into()],
        }
    }

    /// Generate ready-to-use labeling commands
    fn generate_labeling_commands(
        &self,
        entity_ids: &[String],
        suggested_labels: &[String],
    ) -> String {
        if entity_ids.is_empty() || suggested_labels.is_empty() {
            return "No commands to generate".into();
        }

        let first_label = &suggested_labels[0];
        let first_few_entities: Vec<String> = entity_ids.iter().take(3).cloned().collect();

        format!(
            "```\n\
            # Create '{}' label and assign to first few entities:\n\
            create_and_assign_label(name=\"{}\", entity_ids={:?})\n\n\
            # Or assign existing label to individual entities:\n\
            {}\n\
            ```",
            first_label,
            first_label,
            first_few_entities,
            entity_ids
                .iter()
                .take(3)
                .map(|id| format!(
                    "add_labels_to_entity(entity_id=\"{}\", labels=[\"{}\"])",
                    id, first_label
                ))
                .collect::<Vec<_>>()
                .join("\n")
        )
    }

    /// Generate device-specific labeling suggestions
    async fn generate_device_labeling_suggestions(
        &self,
        devices_map: &std::collections::HashMap<String, &serde_json::Value>,
        all_entities: &serde_json::Value,
    ) -> Result<String, McpError> {
        let empty_vec = vec![];
        let entities_array = all_entities.as_array().unwrap_or(&empty_vec);

        let mut output = String::from("## Device-Based Labeling Suggestions\n\n");

        // Find devices with multiple unlabeled entities
        let mut device_entity_count: std::collections::HashMap<String, Vec<String>> =
            std::collections::HashMap::new();

        for entity in entities_array {
            if let Some(device_id) = entity.get("device_id").and_then(|v| v.as_str()) {
                if let Some(entity_id) = entity.get("entity_id").and_then(|v| v.as_str()) {
                    let is_unlabeled = entity
                        .get("labels")
                        .and_then(|labels| labels.as_array())
                        .map(|arr| arr.is_empty())
                        .unwrap_or(true);

                    if is_unlabeled {
                        device_entity_count
                            .entry(device_id.into())
                            .or_default()
                            .push(entity_id.into());
                    }
                }
            }
        }

        // Filter devices with 2+ unlabeled entities
        let multi_entity_devices: Vec<_> = device_entity_count
            .iter()
            .filter(|(_, entities)| entities.len() >= 2)
            .take(10) // Limit to prevent output bloat
            .collect();

        if !multi_entity_devices.is_empty() {
            output.push_str("**Devices with Multiple Unlabeled Entities** (consider labeling all entities from same device):\n\n");

            for (device_id, entity_ids) in multi_entity_devices {
                let device_info = devices_map.get(device_id);
                let device_name = device_info
                    .and_then(|device| {
                        device
                            .get("name_by_user")
                            .or_else(|| device.get("name"))
                            .and_then(|v| v.as_str())
                    })
                    .unwrap_or("Unknown Device");

                let manufacturer = device_info
                    .and_then(|device| device.get("manufacturer").and_then(|v| v.as_str()))
                    .unwrap_or("Unknown");

                // Suggest labels based on device characteristics
                let suggested_labels =
                    self.suggest_labels_for_device(device_info.copied(), entity_ids);

                output.push_str(&format!(
                    "### {} ({}) - {} entities\n\
                    - **Device ID**: `{}`\n\
                    - **Manufacturer**: {}\n\
                    - **Suggested Labels**: {}\n\
                    - **Entity IDs**: {}\n\n\
                    **Command**:\n\
                    ```\n\
                    create_and_assign_label(name=\"{}\", entity_ids={:?})\n\
                    ```\n\n",
                    device_name,
                    &device_id[..8],
                    entity_ids.len(),
                    device_id,
                    manufacturer,
                    suggested_labels.join(", "),
                    entity_ids
                        .iter()
                        .map(|id| format!("`{}`", id))
                        .collect::<Vec<_>>()
                        .join(", "),
                    suggested_labels.first().unwrap_or(&"device_group".into()),
                    entity_ids.iter().take(5).cloned().collect::<Vec<_>>()
                ));
            }
        } else {
            output.push_str(
                "✅ Most devices have well-distributed entities or are already labeled.\n\n",
            );
        }

        Ok(output)
    }

    /// Suggest labels for a specific device based on its characteristics
    fn suggest_labels_for_device(
        &self,
        device_info: Option<&serde_json::Value>,
        entity_ids: &[String],
    ) -> Vec<String> {
        let mut suggestions = Vec::new();

        // Analyze device characteristics
        if let Some(device) = device_info {
            // Check manufacturer for specific suggestions
            if let Some(manufacturer) = device.get("manufacturer").and_then(|v| v.as_str()) {
                match manufacturer.to_lowercase().as_str() {
                    "philips" => suggestions.push("lighting".into()),
                    "nest" | "ecobee" => suggestions.push("climate".into()),
                    "ring" | "arlo" => suggestions.push("security".into()),
                    "sonos" | "bose" => suggestions.push("audio".into()),
                    "tesla" => suggestions.push("automotive".into()),
                    _ => {}
                }
            }

            // Check device name for hints
            if let Some(name) = device
                .get("name_by_user")
                .or_else(|| device.get("name"))
                .and_then(|v| v.as_str())
            {
                let name_lower = name.to_lowercase();
                if name_lower.contains("light") || name_lower.contains("lamp") {
                    suggestions.push("lighting".into());
                } else if name_lower.contains("lock") || name_lower.contains("door") {
                    suggestions.push("security".into());
                } else if name_lower.contains("sensor") {
                    suggestions.push("monitoring".into());
                } else if name_lower.contains("climate") || name_lower.contains("thermostat") {
                    suggestions.push("climate".into());
                }
            }
        }

        // Analyze entity patterns
        let has_light = entity_ids.iter().any(|id| id.starts_with("light."));
        let has_sensor = entity_ids.iter().any(|id| id.starts_with("sensor."));
        let has_binary_sensor = entity_ids.iter().any(|id| id.starts_with("binary_sensor."));
        let has_switch = entity_ids.iter().any(|id| id.starts_with("switch."));

        if has_light && !suggestions.contains(&"lighting".into()) {
            suggestions.push("lighting".into());
        }
        if (has_sensor || has_binary_sensor) && !suggestions.contains(&"monitoring".into()) {
            suggestions.push("monitoring".into());
        }
        if has_switch && !suggestions.contains(&"control".into()) {
            suggestions.push("control".into());
        }

        // Default suggestions if none found
        if suggestions.is_empty() {
            suggestions.push("device_group".into());
            suggestions.push("uncategorized".into());
        }

        suggestions
    }

    /// Generate configuration recommendations
    fn generate_recommendations(
        &self,
        unassigned_entities: usize,
        unassigned_devices: usize,
        areas_without_floor: usize,
        floor_count: usize,
    ) -> String {
        let mut recommendations = Vec::new();

        if unassigned_entities > 0 {
            recommendations.push(format!(
                "🏠 Assign {} unassigned entities to areas using `assign_entity_to_area()`",
                unassigned_entities
            ));
        }

        if unassigned_devices > 0 {
            recommendations.push(format!(
                "📱 Assign {} unassigned devices to areas using `assign_device_to_area()`",
                unassigned_devices
            ));
        }

        if areas_without_floor > 0 && floor_count == 0 {
            recommendations.push("🏢 Consider creating floors using `create_floor()` to organize your areas hierarchically".into());
        } else if areas_without_floor > 0 {
            recommendations.push(format!(
                "🏢 Assign {} areas to floors using `update_area()`",
                areas_without_floor
            ));
        }

        recommendations
            .push("🏷️ Use `recommend_labeling()` to organize entities with labels".into());

        if recommendations.is_empty() {
            recommendations.push("✅ Your Home Assistant configuration is well organized!".into());
        }

        recommendations.join("\n")
    }

    // =============================================================================
    // State Analysis Methods - Enable answering analytical questions
    // =============================================================================

    /// Get all entity states, optionally filtered by domain
    /// This allows answering questions like "show me all sensor readings"
    async fn get_all_entity_states(
        &self,
        args: GetAllEntityStatesArgs,
    ) -> Result<CallToolResult, McpError> {
        let states = self.registry_ops.client.rest_get("api/states").await?;
        let empty_vec = vec![];
        let all_states = states.as_array().unwrap_or(&empty_vec);

        let filtered_states: Vec<&serde_json::Value> = if let Some(ref domain) = args.domain {
            all_states
                .iter()
                .filter(|state| {
                    state
                        .get("entity_id")
                        .and_then(|id| id.as_str())
                        .map(|id| id.starts_with(&format!("{}.", domain)))
                        .unwrap_or(false)
                })
                .collect()
        } else {
            all_states.iter().collect()
        };

        // Transform the response based on include_attributes flag
        let result = if args.include_attributes {
            json!(filtered_states)
        } else {
            // Return simplified format without attributes
            let simplified: Vec<serde_json::Value> = filtered_states
                .iter()
                .map(|state| {
                    json!({
                        "entity_id": state.get("entity_id"),
                        "state": state.get("state"),
                        "last_changed": state.get("last_changed"),
                        "last_updated": state.get("last_updated")
                    })
                })
                .collect();
            json!(simplified)
        };

        Ok(CallToolResult::success(vec![Content::text(
            serde_json::to_string_pretty(&result)
                .map_err(|e| McpError::internal_error(e.to_string(), None))?,
        )]))
    }

    /// Get all entity states for a specific device
    /// This enables device-specific analysis like "show me all readings from this sensor"
    async fn get_device_states(
        &self,
        args: GetDeviceStatesArgs,
    ) -> Result<CallToolResult, McpError> {
        // First get all entities for this device
        let all_entities = self.registry_ops.list("entity_registry").await?;
        let empty_vec = vec![];
        let entities = all_entities.as_array().unwrap_or(&empty_vec);

        let device_entity_ids: Vec<String> = entities
            .iter()
            .filter(|entity| {
                entity
                    .get("device_id")
                    .and_then(|id| id.as_str())
                    .map(|id| id == args.device_id)
                    .unwrap_or(false)
            })
            .filter_map(|entity| {
                entity
                    .get("entity_id")
                    .and_then(|id| id.as_str())
                    .map(|id| id.to_string())
            })
            .collect();

        if device_entity_ids.is_empty() {
            return Ok(CallToolResult::success(vec![Content::text(format!(
                "No entities found for device: {}",
                args.device_id
            ))]));
        }

        // Get states for all device entities
        let states = self.registry_ops.client.rest_get("api/states").await?;
        let all_states = states.as_array().unwrap_or(&empty_vec);

        let device_states: Vec<&serde_json::Value> = all_states
            .iter()
            .filter(|state| {
                state
                    .get("entity_id")
                    .and_then(|id| id.as_str())
                    .map(|id| device_entity_ids.contains(&id.to_string()))
                    .unwrap_or(false)
            })
            .collect();

        let result = if args.include_attributes {
            json!({
                "device_id": args.device_id,
                "entity_count": device_states.len(),
                "states": device_states
            })
        } else {
            let simplified: Vec<serde_json::Value> = device_states
                .iter()
                .map(|state| {
                    json!({
                        "entity_id": state.get("entity_id"),
                        "state": state.get("state"),
                        "last_updated": state.get("last_updated")
                    })
                })
                .collect();
            json!({
                "device_id": args.device_id,
                "entity_count": simplified.len(),
                "states": simplified
            })
        };

        Ok(CallToolResult::success(vec![Content::text(
            serde_json::to_string_pretty(&result)
                .map_err(|e| McpError::internal_error(e.to_string(), None))?,
        )]))
    }

    /// Analyze Zigbee network topology and signal strength
    /// This enables answering questions like "which device is furthest from coordinator"
    async fn analyze_zigbee_network(
        &self,
        _args: AnalyzeZigbeeNetworkArgs,
    ) -> Result<CallToolResult, McpError> {
        let mut analysis = json!({
            "network_type": "zigbee",
            "analysis_timestamp": chrono::Utc::now().to_rfc3339(),
            "devices": [],
            "summary": {}
        });

        // Try to detect Zigbee integration (ZHA vs Zigbee2MQTT)
        let states = self.registry_ops.client.rest_get("api/states").await?;
        let empty_vec = vec![];
        let all_states = states.as_array().unwrap_or(&empty_vec);

        // Look for ZHA coordinator
        let mut zha_devices = Vec::new();
        let mut zigbee2mqtt_devices = Vec::new();

        for state in all_states {
            if let Some(entity_id) = state.get("entity_id").and_then(|id| id.as_str()) {
                // Check for ZHA entities (usually have _ieee in attributes)
                if let Some(attributes) = state.get("attributes").and_then(|attr| attr.as_object())
                {
                    if attributes.contains_key("ieee")
                        || attributes.contains_key("nwk")
                        || attributes.contains_key("lqi")
                        || attributes.contains_key("rssi")
                    {
                        zha_devices.push(json!({
                            "entity_id": entity_id,
                            "state": state.get("state"),
                            "ieee": attributes.get("ieee"),
                            "nwk": attributes.get("nwk"),
                            "lqi": attributes.get("lqi"),
                            "rssi": attributes.get("rssi"),
                            "route": attributes.get("route"),
                            "manufacturer": attributes.get("manufacturer"),
                            "model": attributes.get("model"),
                            "last_seen": attributes.get("last_seen")
                        }));
                    }
                }

                // Check for Zigbee2MQTT entities (usually under zigbee2mqtt domain or have specific attributes)
                if entity_id.starts_with("sensor.") && entity_id.contains("zigbee2mqtt") {
                    if let Some(attributes) =
                        state.get("attributes").and_then(|attr| attr.as_object())
                    {
                        zigbee2mqtt_devices.push(json!({
                            "entity_id": entity_id,
                            "state": state.get("state"),
                            "linkquality": attributes.get("linkquality"),
                            "last_seen": attributes.get("last_seen"),
                            "attributes": attributes
                        }));
                    }
                }
            }
        }

        // Analyze based on available data
        if !zha_devices.is_empty() {
            analysis["integration"] = json!("ZHA");
            analysis["devices"] = json!(zha_devices);

            // Find device with lowest LQI (furthest)
            let mut lowest_lqi = 255u32;
            let mut furthest_device: Option<String> = None;

            for device in &zha_devices {
                if let Some(lqi) = device.get("lqi").and_then(|l| l.as_u64()) {
                    if (lqi as u32) < lowest_lqi {
                        lowest_lqi = lqi as u32;
                        furthest_device = device
                            .get("entity_id")
                            .and_then(|id| id.as_str())
                            .map(|s| s.to_string());
                    }
                }
            }

            analysis["summary"] = json!({
                "total_devices": zha_devices.len(),
                "integration": "ZHA",
                "furthest_device": furthest_device,
                "lowest_lqi": if lowest_lqi < 255 { Some(lowest_lqi) } else { None }
            });
        } else if !zigbee2mqtt_devices.is_empty() {
            analysis["integration"] = json!("Zigbee2MQTT");
            analysis["devices"] = json!(zigbee2mqtt_devices);

            // Find device with lowest link quality (furthest)
            let mut lowest_quality = 255u32;
            let mut furthest_device: Option<String> = None;

            for device in &zigbee2mqtt_devices {
                if let Some(quality) = device.get("linkquality").and_then(|q| q.as_u64()) {
                    if (quality as u32) < lowest_quality {
                        lowest_quality = quality as u32;
                        furthest_device = device
                            .get("entity_id")
                            .and_then(|id| id.as_str())
                            .map(|s| s.to_string());
                    }
                }
            }

            analysis["summary"] = json!({
                "total_devices": zigbee2mqtt_devices.len(),
                "integration": "Zigbee2MQTT",
                "furthest_device": furthest_device,
                "lowest_link_quality": if lowest_quality < 255 { Some(lowest_quality) } else { None }
            });
        } else {
            analysis["summary"] = json!({
                "error": "No Zigbee devices detected. Make sure ZHA or Zigbee2MQTT is configured and running.",
                "suggestion": "Check if you have zigbee devices paired and entities created in Home Assistant"
            });
        }

        Ok(CallToolResult::success(vec![Content::text(format!(
            "# Zigbee Network Analysis\n\n```json\n{}\n```\n\n## Key Findings\n{}",
            serde_json::to_string_pretty(&analysis)
                .map_err(|e| McpError::internal_error(e.to_string(), None))?,
            if let Some(furthest) = analysis["summary"].get("furthest_device") {
                format!(
                    "**Furthest device from coordinator**: {}",
                    furthest.as_str().unwrap_or("unknown")
                )
            } else {
                "No signal strength data available. Make sure Zigbee devices are properly configured.".to_string()
            }
        ))]))
    }
}

impl ServerHandler for HomeAssistantService {
    fn get_info(&self) -> ServerInfo {
        ServerInfo {
            protocol_version: ProtocolVersion::V_2024_11_05,
            capabilities: ServerCapabilities::builder().enable_tools().build(),
            server_info: Implementation::from_build_env(),
            instructions: Some(
                "This server provides access to Home Assistant configuration resources.".into(),
            ),
        }
    }

    async fn initialize(
        &self,
        _request: InitializeRequestParam,
        context: RequestContext<RoleServer>,
    ) -> Result<InitializeResult, McpError> {
        if let Some(http_request_part) = context.extensions.get::<axum::http::request::Parts>() {
            let initialize_headers = &http_request_part.headers;
            let initialize_uri = &http_request_part.uri;
            tracing::info!(?initialize_headers, %initialize_uri, "initialize from http server");
        }
        Ok(self.get_info())
    }

    async fn list_tools(
        &self,
        _request: Option<PaginatedRequestParam>,
        _context: RequestContext<RoleServer>,
    ) -> Result<ListToolsResult, McpError> {
        let tools = vec![
            Tool {
                name: "describe_resource".into(),
                description: Some("Get schema for a Home Assistant resource".into()),
                input_schema: Arc::new(schema_for_type::<DescribeResourceArgs>()),
                annotations: None,
            },
            Tool {
                name: "registry_list".into(),
                description: Some("List items from any Home Assistant registry".into()),
                input_schema: Arc::new(schema_for_type::<RegistryListArgs>()),
                annotations: None,
            },
            Tool {
                name: "entity_find".into(),
                description: Some("Find entities with filtering".into()),
                input_schema: Arc::new(schema_for_type::<EntityFindArgs>()),
                annotations: None,
            },
            Tool {
                name: "entity_get".into(),
                description: Some("Get detailed information about a specific entity".into()),
                input_schema: Arc::new(schema_for_type::<EntityGetArgs>()),
                annotations: None,
            },
            Tool {
                name: "device_get".into(),
                description: Some("Get detailed information about a specific device".into()),
                input_schema: Arc::new(schema_for_type::<DeviceGetArgs>()),
                annotations: None,
            },
            Tool {
                name: "entity_assign_area".into(),
                description: Some("Assign an entity to an area".into()),
                input_schema: Arc::new(schema_for_type::<EntityAssignAreaArgs>()),
                annotations: None,
            },
            Tool {
                name: "entity_add_labels".into(),
                description: Some("Add labels to an entity".into()),
                input_schema: Arc::new(schema_for_type::<EntityAddLabelsArgs>()),
                annotations: None,
            },
            Tool {
                name: "entity_remove_labels".into(),
                description: Some("Remove labels from an entity".into()),
                input_schema: Arc::new(schema_for_type::<EntityRemoveLabelsArgs>()),
                annotations: None,
            },
            Tool {
                name: "device_get_entities".into(),
                description: Some("Get all entities for a specific device".into()),
                input_schema: Arc::new(schema_for_type::<DeviceGetEntitiesArgs>()),
                annotations: None,
            },
            Tool {
                name: "create_label".into(),
                description: Some("Create a new label".into()),
                input_schema: Arc::new(schema_for_type::<CreateLabelArgs>()),
                annotations: None,
            },
            Tool {
                name: "update_label".into(),
                description: Some("Update an existing label".into()),
                input_schema: Arc::new(schema_for_type::<UpdateLabelArgs>()),
                annotations: None,
            },
            Tool {
                name: "create_and_label_device".into(),
                description: Some("Create a label and apply it to a device".into()),
                input_schema: Arc::new(schema_for_type::<CreateAndLabelDeviceArgs>()),
                annotations: None,
            },
            Tool {
                name: "create_and_assign_label".into(),
                description: Some("Create a label and assign it to entities".into()),
                input_schema: Arc::new(schema_for_type::<CreateAndAssignLabelArgs>()),
                annotations: None,
            },
            Tool {
                name: "get_all_entity_states".into(),
                description: Some("Get current states of all entities, optionally filtered by domain. Essential for analysis and answering questions about current device readings.".into()),
                input_schema: Arc::new(schema_for_type::<GetAllEntityStatesArgs>()),
                annotations: None,
            },
            Tool {
                name: "get_device_states".into(),
                description: Some("Get current states of all entities for a specific device. Useful for device-specific analysis and troubleshooting.".into()),
                input_schema: Arc::new(schema_for_type::<GetDeviceStatesArgs>()),
                annotations: None,
            },
            Tool {
                name: "analyze_zigbee_network".into(),
                description: Some("Analyze Zigbee network topology and signal strength. Can answer questions like 'which device is furthest from coordinator' by examining LQI/RSSI data.".into()),
                input_schema: Arc::new(schema_for_type::<AnalyzeZigbeeNetworkArgs>()),
                annotations: None,
            },
            Tool {
                name: "list_devices".into(),
                description: Some("List devices with optional filtering by area or manufacturer.".into()),
                input_schema: Arc::new(schema_for_type::<ListDevicesArgs>()),
                annotations: None,
            },
            Tool {
                name: "update_area".into(),
                description: Some("Update area properties like name, floor assignment, and aliases.".into()),
                input_schema: Arc::new(schema_for_type::<UpdateAreaArgs>()),
                annotations: None,
            },
            Tool {
                name: "update_device".into(),
                description: Some("Update device properties like name and area assignment.".into()),
                input_schema: Arc::new(schema_for_type::<UpdateDeviceArgs>()),
                annotations: None,
            },
            Tool {
                name: "update_entity".into(),
                description: Some("Update entity properties like name, area assignment, and icon.".into()),
                input_schema: Arc::new(schema_for_type::<UpdateEntityArgs>()),
                annotations: None,
            },
            Tool {
                name: "create_area".into(),
                description: Some("Create a new area with optional floor assignment and aliases.".into()),
                input_schema: Arc::new(schema_for_type::<CreateAreaArgs>()),
                annotations: None,
            },
            Tool {
                name: "create_floor".into(),
                description: Some("Create a new floor with optional level and aliases.".into()),
                input_schema: Arc::new(schema_for_type::<CreateFloorArgs>()),
                annotations: None,
            },
            Tool {
                name: "delete_area".into(),
                description: Some("Delete an area from the registry.".into()),
                input_schema: Arc::new(schema_for_type::<DeleteAreaArgs>()),
                annotations: None,
            },
            Tool {
                name: "delete_floor".into(),
                description: Some("Delete a floor from the registry.".into()),
                input_schema: Arc::new(schema_for_type::<DeleteFloorArgs>()),
                annotations: None,
            },
            Tool {
                name: "delete_label".into(),
                description: Some("Delete a label from the registry.".into()),
                input_schema: Arc::new(schema_for_type::<DeleteLabelArgs>()),
                annotations: None,
            },
            Tool {
                name: "assign_entity_to_area".into(),
                description: Some("Assign an entity to a specific area.".into()),
                input_schema: Arc::new(schema_for_type::<AssignEntityToAreaArgs>()),
                annotations: None,
            },
            Tool {
                name: "unassign_entity_from_area".into(),
                description: Some("Remove entity from its current area assignment.".into()),
                input_schema: Arc::new(schema_for_type::<UnassignEntityFromAreaArgs>()),
                annotations: None,
            },
            Tool {
                name: "assign_device_to_area".into(),
                description: Some("Assign a device to a specific area.".into()),
                input_schema: Arc::new(schema_for_type::<AssignDeviceToAreaArgs>()),
                annotations: None,
            },
            Tool {
                name: "add_labels_to_entity".into(),
                description: Some("Add labels to an entity.".into()),
                input_schema: Arc::new(schema_for_type::<AddLabelsToEntityArgs>()),
                annotations: None,
            },
            Tool {
                name: "clear_entity_labels".into(),
                description: Some("Remove all labels from an entity.".into()),
                input_schema: Arc::new(schema_for_type::<ClearEntityLabelsArgs>()),
                annotations: None,
            },
            Tool {
                name: "get_summary".into(),
                description: Some("Get a summary of the Home Assistant configuration including entity and device counts.".into()),
                input_schema: Arc::new(schema_for_type::<GetSummaryArgs>()),
                annotations: None,
            },
            Tool {
                name: "get_entities_in_area".into(),
                description: Some("Get all entities in a specific area, optionally filtered by domain.".into()),
                input_schema: Arc::new(schema_for_type::<GetEntitiesInAreaArgs>()),
                annotations: None,
            },
            Tool {
                name: "get_unassigned_entities".into(),
                description: Some("Get entities that are not assigned to any area.".into()),
                input_schema: Arc::new(schema_for_type::<GetUnassignedEntitiesArgs>()),
                annotations: None,
            },
            Tool {
                name: "find_area_by_name".into(),
                description: Some("Find areas by name with optional fuzzy matching.".into()),
                input_schema: Arc::new(schema_for_type::<FindAreaByNameArgs>()),
                annotations: None,
            },
            Tool {
                name: "find_floor_by_name".into(),
                description: Some("Find floors by name with optional fuzzy matching.".into()),
                input_schema: Arc::new(schema_for_type::<FindFloorByNameArgs>()),
                annotations: None,
            },
            Tool {
                name: "search_entities".into(),
                description: Some("Search entities by name with optional domain filtering and fuzzy matching.".into()),
                input_schema: Arc::new(schema_for_type::<SearchEntitiesArgs>()),
                annotations: None,
            },
            Tool {
                name: "find_entities_by_label".into(),
                description: Some("Find entities that have a specific label.".into()),
                input_schema: Arc::new(schema_for_type::<FindEntitiesByLabelArgs>()),
                annotations: None,
            },
            Tool {
                name: "discover_resources".into(),
                description: Some("Discover available Home Assistant resources and their schemas.".into()),
                input_schema: Arc::new(schema_for_type::<DiscoverResourcesArgs>()),
                annotations: None,
            },
            Tool {
                name: "discover_tools".into(),
                description: Some("Discover available MCP tools and their capabilities.".into()),
                input_schema: Arc::new(schema_for_type::<DiscoverToolsArgs>()),
                annotations: None,
            },
            Tool {
                name: "validate_entity_id".into(),
                description: Some("Validate that an entity ID exists and get its information.".into()),
                input_schema: Arc::new(schema_for_type::<ValidateEntityIdArgs>()),
                annotations: None,
            },
            Tool {
                name: "validate_device_id".into(),
                description: Some("Validate that a device ID exists and get its information.".into()),
                input_schema: Arc::new(schema_for_type::<ValidateDeviceIdArgs>()),
                annotations: None,
            },
            Tool {
                name: "get_entities_for_device".into(),
                description: Some("Get all entities associated with a specific device.".into()),
                input_schema: Arc::new(schema_for_type::<GetEntitiesForDeviceArgs>()),
                annotations: None,
            },
            Tool {
                name: "get_device_summary".into(),
                description: Some("Get detailed summary information about a specific device.".into()),
                input_schema: Arc::new(schema_for_type::<GetDeviceSummaryArgs>()),
                annotations: None,
            },
            Tool {
                name: "analyze_configuration".into(),
                description: Some("Analyze the Home Assistant configuration and provide recommendations.".into()),
                input_schema: Arc::new(schema_for_type::<AnalyzeConfigurationArgs>()),
                annotations: None,
            },
            Tool {
                name: "find_orphaned_entities".into(),
                description: Some("Find entities that are not assigned to areas or devices.".into()),
                input_schema: Arc::new(schema_for_type::<FindOrphanedEntitiesArgs>()),
                annotations: None,
            },
            Tool {
                name: "recommend_labels".into(),
                description: Some("Generate label recommendations for entities based on their domains and patterns.".into()),
                input_schema: Arc::new(schema_for_type::<RecommendLabelsArgs>()),
                annotations: None,
            },
        ];

        Ok(ListToolsResult {
            tools,
            next_cursor: None,
        })
    }

    async fn call_tool(
        &self,
        request: CallToolRequestParam,
        _context: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, McpError> {
        let arguments = request.arguments.unwrap_or_default();

        let tool_result = match request.name.as_ref() {
            "describe_resource" => {
                let args: DescribeResourceArgs = serde_json::from_value(serde_json::Value::Object(arguments))
                    .map_err(|e| McpError::invalid_params(
                        format!("describe_resource: Invalid arguments - {}. Expected: {{\"resource\": \"area_registry|device_registry|entity_registry|floor_registry|label_registry|state_object\"}}", e), 
                        None
                    ))?;
                self.describe_resource(args).await
            }
            "registry_list" => {
                let args: RegistryListArgs = serde_json::from_value(serde_json::Value::Object(arguments))
                    .map_err(|e| McpError::invalid_params(
                        format!("registry_list: Invalid arguments - {}. Expected: {{\"resource\": \"area_registry|device_registry|entity_registry|floor_registry|label_registry\", \"limit\": number (optional)}}", e), 
                        None
                    ))?;
                self.registry_list(args).await
            }
            "entity_find" => {
                let args: EntityFindArgs = serde_json::from_value(serde_json::Value::Object(arguments))
                    .map_err(|e| McpError::invalid_params(
                        format!("entity_find: Invalid arguments - {}. Expected: {{\"domain\": \"string (optional)\", \"area_id\": \"string (optional)\", \"pattern\": \"string (optional)\", \"limit\": number (optional)}}", e), 
                        None
                    ))?;
                self.entity_find(args).await
            }
            "entity_get" => {
                let args: EntityGetArgs = serde_json::from_value(serde_json::Value::Object(arguments))
                    .map_err(|e| McpError::invalid_params(
                        format!("entity_get: Invalid arguments - {}. Expected: {{\"entity_id\": \"string\"}}", e), 
                        None
                    ))?;
                self.entity_get(args).await
            }
            "device_get" => {
                let args: DeviceGetArgs = serde_json::from_value(serde_json::Value::Object(arguments))
                    .map_err(|e| McpError::invalid_params(
                        format!("device_get: Invalid arguments - {}. Expected: {{\"device_id\": \"string\"}}", e), 
                        None
                    ))?;
                self.device_get(args).await
            }
            "entity_assign_area" => {
                let args: EntityAssignAreaArgs = serde_json::from_value(serde_json::Value::Object(arguments))
                    .map_err(|e| McpError::invalid_params(
                        format!("entity_assign_area: Invalid arguments - {}. Expected: {{\"entity_id\": \"string\", \"area_id\": \"string\"}}", e), 
                        None
                    ))?;
                self.entity_assign_area(args).await
            }
            "entity_add_labels" => {
                let args: EntityAddLabelsArgs = serde_json::from_value(serde_json::Value::Object(arguments))
                    .map_err(|e| McpError::invalid_params(
                        format!("entity_add_labels: Invalid arguments - {}. Expected: {{\"entity_id\": \"string\", \"labels\": [\"string1\", \"string2\"]}}", e), 
                        None
                    ))?;
                self.entity_add_labels(args).await
            }
            "entity_remove_labels" => {
                let args: EntityRemoveLabelsArgs = serde_json::from_value(serde_json::Value::Object(arguments))
                    .map_err(|e| McpError::invalid_params(
                        format!("entity_remove_labels: Invalid arguments - {}. Expected: {{\"entity_id\": \"string\", \"labels\": [\"string1\", \"string2\"] (optional - omit to remove all)}}", e), 
                        None
                    ))?;
                self.entity_remove_labels(args).await
            }
            "device_get_entities" => {
                let args: DeviceGetEntitiesArgs = serde_json::from_value(serde_json::Value::Object(arguments))
                    .map_err(|e| McpError::invalid_params(
                        format!("device_get_entities: Invalid arguments - {}. Expected: {{\"device_id\": \"string\"}}", e), 
                        None
                    ))?;
                self.device_get_entities(args).await
            }
            "create_label" => {
                let args: CreateLabelArgs = serde_json::from_value(serde_json::Value::Object(arguments))
                    .map_err(|e| McpError::invalid_params(
                        format!("create_label: Invalid arguments - {}. Expected: {{\"name\": \"string\", \"icon\": \"string (optional)\", \"color\": \"string (optional)\", \"description\": \"string (optional)\"}}", e), 
                        None
                    ))?;
                self.create_label(args).await
            }
            "update_label" => {
                let args: UpdateLabelArgs = serde_json::from_value(serde_json::Value::Object(arguments))
                    .map_err(|e| McpError::invalid_params(
                        format!("update_label: Invalid arguments - {}. Expected: {{\"label_id\": \"string\", \"name\": \"string (optional)\", \"icon\": \"string (optional)\", \"color\": \"string (optional)\", \"description\": \"string (optional)\"}}", e), 
                        None
                    ))?;
                self.update_label(args).await
            }
            "create_and_label_device" => {
                let args: CreateAndLabelDeviceArgs = serde_json::from_value(serde_json::Value::Object(arguments))
                    .map_err(|e| McpError::invalid_params(
                        format!("create_and_label_device: Invalid arguments - {}. Expected: {{\"device_id\": \"string\", \"label_name\": \"string\", \"icon\": \"string (optional)\", \"color\": \"string (optional)\", \"description\": \"string (optional)\"}}", e), 
                        None
                    ))?;
                self.create_and_label_device(args).await
            }
            "create_and_assign_label" => {
                let args: CreateAndAssignLabelArgs = serde_json::from_value(serde_json::Value::Object(arguments))
                    .map_err(|e| McpError::invalid_params(
                        format!("create_and_assign_label: Invalid arguments - {}. Expected: {{\"name\": \"string\", \"entity_ids\": [\"string1\", \"string2\"], \"icon\": \"string (optional)\", \"color\": \"string (optional)\", \"description\": \"string (optional)\"}}", e), 
                        None
                    ))?;
                self.create_and_assign_label(args).await
            }
            "get_all_entity_states" => {
                let args: GetAllEntityStatesArgs = serde_json::from_value(serde_json::Value::Object(arguments))
                    .map_err(|e| McpError::invalid_params(
                        format!("get_all_entity_states: Invalid arguments - {}. Expected: {{\"domain\": \"string (optional)\", \"include_attributes\": boolean}}", e), 
                        None
                    ))?;
                self.get_all_entity_states(args).await
            }
            "get_device_states" => {
                let args: GetDeviceStatesArgs = serde_json::from_value(serde_json::Value::Object(arguments))
                    .map_err(|e| McpError::invalid_params(
                        format!("get_device_states: Invalid arguments - {}. Expected: {{\"device_id\": \"string\", \"include_attributes\": boolean}}", e), 
                        None
                    ))?;
                self.get_device_states(args).await
            }
            "analyze_zigbee_network" => {
                let args: AnalyzeZigbeeNetworkArgs = serde_json::from_value(serde_json::Value::Object(arguments))
                    .map_err(|e| McpError::invalid_params(
                        format!("analyze_zigbee_network: Invalid arguments - {}. Expected: {{\"include_signal_data\": boolean, \"include_topology\": boolean}}", e), 
                        None
                    ))?;
                self.analyze_zigbee_network(args).await
            }
            "list_devices" => {
                let args: ListDevicesArgs = serde_json::from_value(serde_json::Value::Object(arguments))
                    .map_err(|e| McpError::invalid_params(
                        format!("list_devices: Invalid arguments - {}. Expected: {{\"area_id\": \"string (optional)\", \"manufacturer\": \"string (optional)\", \"limit\": number (optional)}}", e), 
                        None
                    ))?;
                self.list_devices(args).await
            }
            "update_area" => {
                let args: UpdateAreaArgs = serde_json::from_value(serde_json::Value::Object(arguments))
                    .map_err(|e| McpError::invalid_params(
                        format!("update_area: Invalid arguments - {}. Expected: {{\"area_id\": \"string\", \"name\": \"string (optional)\", \"floor_id\": \"string (optional)\", \"aliases\": [\"string\"] (optional), \"icon\": \"string (optional)\"}}", e), 
                        None
                    ))?;
                self.update_area(args.area_id, args.name, args.floor_id, args.icon)
                    .await
            }
            "update_device" => {
                let args: UpdateDeviceArgs = serde_json::from_value(serde_json::Value::Object(arguments))
                    .map_err(|e| McpError::invalid_params(
                        format!("update_device: Invalid arguments - {}. Expected: {{\"device_id\": \"string\", \"name\": \"string (optional)\", \"area_id\": \"string (optional)\", \"name_by_user\": \"string (optional)\"}}", e), 
                        None
                    ))?;
                self.update_device(args.device_id, args.name, args.area_id)
                    .await
            }
            "update_entity" => {
                let args: UpdateEntityArgs = serde_json::from_value(serde_json::Value::Object(arguments))
                    .map_err(|e| McpError::invalid_params(
                        format!("update_entity: Invalid arguments - {}. Expected: {{\"entity_id\": \"string\", \"name\": \"string (optional)\", \"area_id\": \"string (optional)\", \"icon\": \"string (optional)\"}}", e), 
                        None
                    ))?;
                self.update_entity(args.entity_id, args.name, args.area_id, args.icon, None)
                    .await
            }
            "create_area" => {
                let args: CreateAreaArgs = serde_json::from_value(serde_json::Value::Object(arguments))
                    .map_err(|e| McpError::invalid_params(
                        format!("create_area: Invalid arguments - {}. Expected: {{\"name\": \"string\", \"floor_id\": \"string (optional)\", \"aliases\": [\"string\"] (optional), \"icon\": \"string (optional)\"}}", e), 
                        None
                    ))?;
                self.create_area(args.name, args.floor_id, args.icon, args.aliases)
                    .await
            }
            "create_floor" => {
                let args: CreateFloorArgs = serde_json::from_value(serde_json::Value::Object(arguments))
                    .map_err(|e| McpError::invalid_params(
                        format!("create_floor: Invalid arguments - {}. Expected: {{\"name\": \"string\", \"level\": number (optional), \"aliases\": [\"string\"] (optional), \"icon\": \"string (optional)\"}}", e), 
                        None
                    ))?;
                self.create_floor(args.name, args.level.unwrap_or(0), args.icon, args.aliases)
                    .await
            }
            "delete_area" => {
                let args: DeleteAreaArgs = serde_json::from_value(serde_json::Value::Object(arguments))
                    .map_err(|e| McpError::invalid_params(
                        format!("delete_area: Invalid arguments - {}. Expected: {{\"area_id\": \"string\"}}", e), 
                        None
                    ))?;
                self.delete_area(args.area_id).await
            }
            "delete_floor" => {
                let args: DeleteFloorArgs = serde_json::from_value(serde_json::Value::Object(arguments))
                    .map_err(|e| McpError::invalid_params(
                        format!("delete_floor: Invalid arguments - {}. Expected: {{\"floor_id\": \"string\"}}", e), 
                        None
                    ))?;
                self.delete_floor(args.floor_id).await
            }
            "delete_label" => {
                let args: DeleteLabelArgs = serde_json::from_value(serde_json::Value::Object(arguments))
                    .map_err(|e| McpError::invalid_params(
                        format!("delete_label: Invalid arguments - {}. Expected: {{\"label_id\": \"string\"}}", e), 
                        None
                    ))?;
                self.delete_label(args.label_id).await
            }
            "assign_entity_to_area" => {
                let args: AssignEntityToAreaArgs = serde_json::from_value(serde_json::Value::Object(arguments))
                    .map_err(|e| McpError::invalid_params(
                        format!("assign_entity_to_area: Invalid arguments - {}. Expected: {{\"entity_id\": \"string\", \"area_id\": \"string\"}}", e), 
                        None
                    ))?;
                self.assign_entity_to_area(args.entity_id, args.area_id)
                    .await
            }
            "unassign_entity_from_area" => {
                let args: UnassignEntityFromAreaArgs = serde_json::from_value(serde_json::Value::Object(arguments))
                    .map_err(|e| McpError::invalid_params(
                        format!("unassign_entity_from_area: Invalid arguments - {}. Expected: {{\"entity_id\": \"string\"}}", e), 
                        None
                    ))?;
                self.unassign_entity_from_area(args.entity_id).await
            }
            "assign_device_to_area" => {
                let args: AssignDeviceToAreaArgs = serde_json::from_value(serde_json::Value::Object(arguments))
                    .map_err(|e| McpError::invalid_params(
                        format!("assign_device_to_area: Invalid arguments - {}. Expected: {{\"device_id\": \"string\", \"area_id\": \"string\"}}", e), 
                        None
                    ))?;
                self.assign_device_to_area(args.device_id, args.area_id)
                    .await
            }
            "add_labels_to_entity" => {
                let args: AddLabelsToEntityArgs = serde_json::from_value(serde_json::Value::Object(arguments))
                    .map_err(|e| McpError::invalid_params(
                        format!("add_labels_to_entity: Invalid arguments - {}. Expected: {{\"entity_id\": \"string\", \"labels\": [\"string\"]}}", e), 
                        None
                    ))?;
                self.add_labels_to_entity(args.entity_id, args.labels).await
            }
            "clear_entity_labels" => {
                let args: ClearEntityLabelsArgs = serde_json::from_value(serde_json::Value::Object(arguments))
                    .map_err(|e| McpError::invalid_params(
                        format!("clear_entity_labels: Invalid arguments - {}. Expected: {{\"entity_id\": \"string\"}}", e), 
                        None
                    ))?;
                self.clear_entity_labels(args.entity_id).await
            }
            "get_summary" => {
                let _args: GetSummaryArgs = serde_json::from_value(serde_json::Value::Object(arguments))
                    .map_err(|e| McpError::invalid_params(
                        format!("get_summary: Invalid arguments - {}. Expected: {{\"include_devices\": boolean (optional), \"include_entities\": boolean (optional)}}", e), 
                        None
                    ))?;
                self.get_summary().await
            }
            "get_entities_in_area" => {
                let args: GetEntitiesInAreaArgs = serde_json::from_value(serde_json::Value::Object(arguments))
                    .map_err(|e| McpError::invalid_params(
                        format!("get_entities_in_area: Invalid arguments - {}. Expected: {{\"area_id\": \"string\", \"domain\": \"string (optional)\", \"limit\": number (optional)}}", e), 
                        None
                    ))?;
                self.get_entities_in_area(args.area_id).await
            }
            "get_unassigned_entities" => {
                let _args: GetUnassignedEntitiesArgs = serde_json::from_value(serde_json::Value::Object(arguments))
                    .map_err(|e| McpError::invalid_params(
                        format!("get_unassigned_entities: Invalid arguments - {}. Expected: {{\"domain\": \"string (optional)\", \"limit\": number (optional)}}", e), 
                        None
                    ))?;
                self.get_unassigned_entities().await
            }
            "find_area_by_name" => {
                let args: FindAreaByNameArgs = serde_json::from_value(serde_json::Value::Object(arguments))
                    .map_err(|e| McpError::invalid_params(
                        format!("find_area_by_name: Invalid arguments - {}. Expected: {{\"name\": \"string\", \"fuzzy\": boolean (optional)}}", e), 
                        None
                    ))?;
                self.find_area_by_name(args.name).await
            }
            "find_floor_by_name" => {
                let args: FindFloorByNameArgs = serde_json::from_value(serde_json::Value::Object(arguments))
                    .map_err(|e| McpError::invalid_params(
                        format!("find_floor_by_name: Invalid arguments - {}. Expected: {{\"name\": \"string\", \"fuzzy\": boolean (optional)}}", e), 
                        None
                    ))?;
                self.find_floor_by_name(args.name).await
            }
            "search_entities" => {
                let args: SearchEntitiesArgs = serde_json::from_value(serde_json::Value::Object(arguments))
                    .map_err(|e| McpError::invalid_params(
                        format!("search_entities: Invalid arguments - {}. Expected: {{\"query\": \"string\", \"domain\": \"string (optional)\", \"fuzzy\": boolean (optional), \"limit\": number (optional)}}", e), 
                        None
                    ))?;
                self.search_entities(args.query, args.limit).await
            }
            "find_entities_by_label" => {
                let args: FindEntitiesByLabelArgs = serde_json::from_value(serde_json::Value::Object(arguments))
                    .map_err(|e| McpError::invalid_params(
                        format!("find_entities_by_label: Invalid arguments - {}. Expected: {{\"label\": \"string\", \"domain\": \"string (optional)\", \"limit\": number (optional)}}", e), 
                        None
                    ))?;
                self.find_entities_by_label(args.label, args.limit).await
            }
            "discover_resources" => {
                let _args: DiscoverResourcesArgs = serde_json::from_value(serde_json::Value::Object(arguments))
                    .map_err(|e| McpError::invalid_params(
                        format!("discover_resources: Invalid arguments - {}. Expected: {{\"include_schemas\": boolean (optional)}}", e), 
                        None
                    ))?;
                self.discover_resources().await
            }
            "discover_tools" => {
                let _args: DiscoverToolsArgs = serde_json::from_value(serde_json::Value::Object(arguments))
                    .map_err(|e| McpError::invalid_params(
                        format!("discover_tools: Invalid arguments - {}. Expected: {{\"include_descriptions\": boolean (optional)}}", e), 
                        None
                    ))?;
                self.discover_tools().await
            }
            "validate_entity_id" => {
                let args: ValidateEntityIdArgs = serde_json::from_value(serde_json::Value::Object(arguments))
                    .map_err(|e| McpError::invalid_params(
                        format!("validate_entity_id: Invalid arguments - {}. Expected: {{\"entity_id\": \"string\"}}", e), 
                        None
                    ))?;
                self.validate_entity_id(args.entity_id).await
            }
            "validate_device_id" => {
                let args: ValidateDeviceIdArgs = serde_json::from_value(serde_json::Value::Object(arguments))
                    .map_err(|e| McpError::invalid_params(
                        format!("validate_device_id: Invalid arguments - {}. Expected: {{\"device_id\": \"string\"}}", e), 
                        None
                    ))?;
                self.validate_device_id(args.device_id).await
            }
            "get_entities_for_device" => {
                let args: GetEntitiesForDeviceArgs = serde_json::from_value(serde_json::Value::Object(arguments))
                    .map_err(|e| McpError::invalid_params(
                        format!("get_entities_for_device: Invalid arguments - {}. Expected: {{\"device_id\": \"string\"}}", e), 
                        None
                    ))?;
                self.get_entities_for_device(args.device_id).await
            }
            "get_device_summary" => {
                let args: GetDeviceSummaryArgs = serde_json::from_value(serde_json::Value::Object(arguments))
                    .map_err(|e| McpError::invalid_params(
                        format!("get_device_summary: Invalid arguments - {}. Expected: {{\"device_id\": \"string\"}}", e), 
                        None
                    ))?;
                self.get_device_summary(args.device_id).await
            }
            "analyze_configuration" => {
                let _args: AnalyzeConfigurationArgs = serde_json::from_value(serde_json::Value::Object(arguments))
                    .map_err(|e| McpError::invalid_params(
                        format!("analyze_configuration: Invalid arguments - {}. Expected: {{\"include_recommendations\": boolean (optional), \"include_statistics\": boolean (optional)}}", e), 
                        None
                    ))?;
                self.analyze_configuration().await
            }
            "find_orphaned_entities" => {
                let args: FindOrphanedEntitiesArgs = serde_json::from_value(serde_json::Value::Object(arguments))
                    .map_err(|e| McpError::invalid_params(
                        format!("find_orphaned_entities: Invalid arguments - {}. Expected: {{\"limit\": number (optional)}}", e), 
                        None
                    ))?;
                self.find_orphaned_entities(args.limit).await
            }
            "recommend_labels" => {
                let args: RecommendLabelsArgs = serde_json::from_value(serde_json::Value::Object(arguments))
                    .map_err(|e| McpError::invalid_params(
                        format!("recommend_labels: Invalid arguments - {}. Expected: {{\"limit\": number (optional)}}", e), 
                        None
                    ))?;
                self.recommend_labels(args.limit).await
            }
            unknown_tool => {
                let available_tools = vec![
                    "describe_resource",
                    "registry_list",
                    "entity_find",
                    "entity_get",
                    "device_get",
                    "entity_assign_area",
                    "entity_add_labels",
                    "entity_remove_labels",
                    "device_get_entities",
                    "create_label",
                    "update_label",
                    "create_and_label_device",
                    "create_and_assign_label",
                    "get_all_entity_states",
                    "get_device_states",
                    "analyze_zigbee_network",
                ];
                return Err(McpError::invalid_params(
                    format!(
                        "Unknown tool: '{}'. Available tools: {}",
                        unknown_tool,
                        available_tools.join(", ")
                    ),
                    None,
                ));
            }
        };

        // Return the result directly - errors are already properly formatted
        tool_result
    }
}

#[derive(Parser)]
#[command(name = "ha-config")]
#[command(about = "Home Assistant MCP Configuration Server")]
#[command(version = "0.1.0")]
struct Cli {
    /// Home Assistant URL (e.g., http://localhost:8123)
    #[arg(long = "url", env = "HASS_URL")]
    url: String,

    /// Home Assistant long-lived access token
    #[arg(long = "api-key", env = "HASS_API_KEY")]
    api_key: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "debug".into()),
        )
        .with(tracing_subscriber::fmt::layer().with_writer(std::io::stderr))
        .init();

    let cli = Cli::parse();

    let schemas = serde_yaml::from_str(include_str!("../schemas.yaml"))?;
    let config = HomeAssistantConfig::new(cli.url.clone(), cli.api_key);
    let service = HomeAssistantService::new(schemas, config).await?;

    tracing::info!("🚀 Home Assistant MCP Server starting");
    tracing::info!("📡 Home Assistant URL: {}", cli.url);

    if tracing::enabled!(tracing::Level::DEBUG) {
        tracing::debug!("🐛 Debug mode enabled - JSON schemas will be logged on startup");
    }

    let server_service = service
        .serve(stdio())
        .await
        .inspect_err(|error| tracing::error!(%error, "Error serving"))?;

    server_service.waiting().await?;

    Ok(())
}
