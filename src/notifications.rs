use anyhow::Result;
use serde::Serialize;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};
use tracing::{info, warn};
use zbus::object_server::SignalEmitter;
use zbus::Connection;

const OBJ_PATH: &str = "/org/freedesktop/Notifications";
const IFACE: &str = "org.freedesktop.Notifications";

#[derive(Debug, Clone)]
pub struct StoredNotification {
    pub id: u32,
    pub app_name: String,
    pub app_icon: String,
    pub summary: String,
    pub body: String,
    pub urgency: u8,
    pub expire_timeout: i32,
    pub image_path: Option<String>,
    pub actions: Vec<(String, String)>,
    pub has_inline_reply: bool,
    pub inline_reply_placeholder: Option<String>,
}

#[derive(Serialize)]
pub struct ActionJson {
    pub key: String,
    pub label: String,
}

#[derive(Serialize)]
pub struct NotificationJson {
    pub id: u32,
    pub app_name: String,
    pub app_icon: String,
    pub summary: String,
    pub body: String,
    pub urgency: u8,
    pub expire_timeout: i32,
    pub image_path: Option<String>,
    pub actions: Vec<ActionJson>,
    pub has_inline_reply: bool,
    pub inline_reply_placeholder: Option<String>,
}

impl StoredNotification {
    fn to_json(&self) -> NotificationJson {
        NotificationJson {
            id: self.id,
            app_name: self.app_name.clone(),
            app_icon: self.app_icon.clone(),
            summary: self.summary.clone(),
            body: self.body.clone(),
            urgency: self.urgency,
            expire_timeout: self.expire_timeout,
            image_path: self.image_path.clone(),
            actions: self.actions.iter().map(|(k, l)| ActionJson {
                key: k.clone(),
                label: l.clone(),
            }).collect(),
            has_inline_reply: self.has_inline_reply,
            inline_reply_placeholder: self.inline_reply_placeholder.clone(),
        }
    }
}

#[derive(Debug)]
pub enum NotificationCommand {
    Dismiss { id: u32 },
    InvokeAction { id: u32, key: String },
    Reply { id: u32, text: String },
}

pub struct NotificationsServer {
    state: Arc<Mutex<HashMap<u32, StoredNotification>>>,
    tx_out: mpsc::UnboundedSender<String>,
    next_id: AtomicU32,
}

impl NotificationsServer {
    pub fn new(tx_out: mpsc::UnboundedSender<String>) -> Self {
        Self {
            state: Arc::new(Mutex::new(HashMap::new())),
            tx_out,
            next_id: AtomicU32::new(1),
        }
    }

    pub fn state_handle(&self) -> Arc<Mutex<HashMap<u32, StoredNotification>>> {
        self.state.clone()
    }
}

fn parse_image_data(hints: &HashMap<String, zbus::zvariant::OwnedValue>, id: u32) -> Option<String> {
    let raw = hints.get("image-data").or_else(|| hints.get("image_data"))?;
    let sig = raw.value_signature();
    let sig_str = sig.to_string();
    if !sig_str.starts_with('(') {
        return None;
    }
    let tuple: zbus::zvariant::Structure = raw.clone().try_into().ok()?;
    let fields = tuple.fields();
    if fields.len() != 7 {
        return None;
    }
    let width: i32 = fields[0].clone().try_into().ok()?;
    let height: i32 = fields[1].clone().try_into().ok()?;
    let rowstride: i32 = fields[2].clone().try_into().ok()?;
    let has_alpha: bool = fields[3].clone().try_into().ok()?;
    let _bits_per_sample: i32 = fields[4].clone().try_into().ok()?;
    let channels: i32 = fields[5].clone().try_into().ok()?;
    let data_arr: zbus::zvariant::Array = fields[6].clone().try_into().ok()?;

    if width <= 0 || height <= 0 || rowstride <= 0 || channels < 3 {
        return None;
    }

    let mut bytes = Vec::with_capacity((width * height * 4) as usize);
    let raw_bytes: Vec<u8> = data_arr.iter().filter_map(|v| u8::try_from(v).ok()).collect();
    let expected = (rowstride * height) as usize;
    if raw_bytes.len() < expected {
        return None;
    }

    for y in 0..height {
        let row_start = (y * rowstride) as usize;
        for x in 0..width {
            let px = row_start + (x * channels) as usize;
            if px + 3 > raw_bytes.len() {
                return None;
            }
            let a = if has_alpha && channels == 4 { raw_bytes[px + 3] } else { 255 };
            let r = raw_bytes[px];
            let g = raw_bytes[px + 1];
            let b = raw_bytes[px + 2];
            bytes.push(r);
            bytes.push(g);
            bytes.push(b);
            bytes.push(a);
        }
    }

    let buf: image::RgbaImage = image::ImageBuffer::from_raw(width as u32, height as u32, bytes)?;
    let path = format!("/tmp/mshelld-{id}.png");
    if buf.save(&path).is_ok() {
        Some(path)
    } else {
        None
    }
}

#[zbus::interface(name = "org.freedesktop.Notifications")]
impl NotificationsServer {
    #[zbus(signal)]
    async fn notification_closed(emitter: &SignalEmitter<'_>, id: u32, reason: u32) -> zbus::Result<()>;

    #[zbus(signal)]
    async fn action_invoked(emitter: &SignalEmitter<'_>, id: u32, action_key: String) -> zbus::Result<()>;

    #[zbus(signal)]
    async fn notification_replied(emitter: &SignalEmitter<'_>, id: u32, text: String) -> zbus::Result<()>;

    async fn notify(
        &self,
        #[zbus(signal_emitter)] emitter: SignalEmitter<'_>,
        app_name: String,
        replaces_id: u32,
        app_icon: String,
        summary: String,
        body: String,
        actions: Vec<String>,
        hints: HashMap<String, zbus::zvariant::OwnedValue>,
        expire_timeout: i32,
    ) -> u32 {
        let id = if replaces_id != 0 { replaces_id } else {
            self.next_id.fetch_add(1, Ordering::SeqCst)
        };

        let urgency: u8 = hints.get("urgency")
            .and_then(|v| u8::try_from(v.clone()).ok())
            .unwrap_or(1);

        let image_path = hints.get("image-path")
            .or_else(|| hints.get("image_path"))
            .and_then(|v| String::try_from(v.clone()).ok())
            .or_else(|| parse_image_data(&hints, id));

        let mut parsed_actions: Vec<(String, String)> = Vec::new();
        let mut has_inline_reply = false;
        let mut inline_reply_placeholder: Option<String> = None;
        let mut i = 0;
        while i + 1 < actions.len() {
            let key = actions[i].clone();
            let label = actions[i + 1].clone();
            if key == "inline-reply" {
                has_inline_reply = true;
                if !label.is_empty() {
                    inline_reply_placeholder = Some(label);
                }
            } else if !key.is_empty() {
                parsed_actions.push((key, label));
            }
            i += 2;
        }

        if has_inline_reply && inline_reply_placeholder.is_none() {
            if let Some(ph) = hints.get("x-kde-reply-placeholder-text")
                .or_else(|| hints.get("x-kde-reply-placeholder"))
                .and_then(|v| String::try_from(v.clone()).ok())
            {
                inline_reply_placeholder = Some(ph);
            } else {
                inline_reply_placeholder = Some("Reply...".to_string());
            }
        }

        let stored = StoredNotification {
            id,
            app_name: app_name.clone(),
            app_icon: app_icon.clone(),
            summary: summary.clone(),
            body: body.clone(),
            urgency,
            expire_timeout,
            image_path: image_path.clone(),
            actions: parsed_actions.clone(),
            has_inline_reply,
            inline_reply_placeholder: inline_reply_placeholder.clone(),
        };

        let event = if replaces_id != 0 { "update" } else { "add" };
        let json = serde_json::json!({
            "type": "notification",
            "event": event,
            "notification": stored.to_json(),
        });
        let _ = self.tx_out.send(json.to_string());

        {
            let mut map = self.state.lock().await;
            map.insert(id, stored);
        }

        info!("notify id={id} app={app_name} event={event}");

        if expire_timeout > 0 && urgency < 2 {
            let state = self.state.clone();
            let tx_out = self.tx_out.clone();
            let emitter_owned = emitter.to_owned();
            tokio::spawn(async move {
                tokio::time::sleep(std::time::Duration::from_millis(expire_timeout as u64)).await;
                let mut map = state.lock().await;
                if map.remove(&id).is_some() {
                    let _ = Self::notification_closed(&emitter_owned, id, 1).await;
                    let json = serde_json::json!({
                        "type": "notification",
                        "event": "close",
                        "id": id,
                    });
                    let _ = tx_out.send(json.to_string());
                }
            });
        }

        id
    }

    async fn close_notification(
        &self,
        #[zbus(signal_emitter)] emitter: SignalEmitter<'_>,
        id: u32,
    ) {
        let mut map = self.state.lock().await;
        if map.remove(&id).is_some() {
            let _ = Self::notification_closed(&emitter, id, 3).await;
            let json = serde_json::json!({
                "type": "notification",
                "event": "close",
                "id": id,
            });
            let _ = self.tx_out.send(json.to_string());
        }
    }

    async fn get_capabilities(&self) -> Vec<String> {
        vec![
            "body".to_string(),
            "body-markup".to_string(),
            "actions".to_string(),
            "icon-static".to_string(),
            "persistence".to_string(),
        ]
    }

    async fn get_server_information(&self) -> (String, String, String, String) {
        (
            "mshelld".to_string(),
            "mshelld".to_string(),
            "0.1.0".to_string(),
            "1.2".to_string(),
        )
    }
}

pub async fn run(
    conn: Connection,
    state: Arc<Mutex<HashMap<u32, StoredNotification>>>,
    tx_out: mpsc::UnboundedSender<String>,
    mut cmd_rx: mpsc::UnboundedReceiver<NotificationCommand>,
) -> Result<()> {
    info!("notifications server running");

    while let Some(cmd) = cmd_rx.recv().await {
        match cmd {
            NotificationCommand::Dismiss { id } => {
                let mut map = state.lock().await;
                if map.remove(&id).is_some() {
                    let _ = conn.emit_signal(
                        None::<&str>,
                        OBJ_PATH,
                        IFACE,
                        "NotificationClosed",
                        &(id, 3u32),
                    ).await;
                    let json = serde_json::json!({
                        "type": "notification",
                        "event": "close",
                        "id": id,
                    });
                    let _ = tx_out.send(json.to_string());
                }
            }
            NotificationCommand::InvokeAction { id, key } => {
                let mut map = state.lock().await;
                let exists = map.contains_key(&id);
                if exists {
                    let _ = conn.emit_signal(
                        None::<&str>,
                        OBJ_PATH,
                        IFACE,
                        "ActionInvoked",
                        &(id, key.as_str()),
                    ).await;
                    map.remove(&id);
                    let json = serde_json::json!({
                        "type": "notification",
                        "event": "close",
                        "id": id,
                    });
                    let _ = tx_out.send(json.to_string());
                    let _ = conn.emit_signal(
                        None::<&str>,
                        OBJ_PATH,
                        IFACE,
                        "NotificationClosed",
                        &(id, 2u32),
                    ).await;
                } else {
                    warn!("invokeAction: unknown id={id}");
                }
            }
            NotificationCommand::Reply { id, text } => {
                let map = state.lock().await;
                if !map.contains_key(&id) {
                    warn!("reply: unknown id={id}");
                    continue;
                }
                let _ = conn.emit_signal(
                    None::<&str>,
                    OBJ_PATH,
                    IFACE,
                    "NotificationReplied",
                    &(id, text.as_str()),
                ).await;
                let combined = format!("inline-reply::{text}");
                let _ = conn.emit_signal(
                    None::<&str>,
                    OBJ_PATH,
                    IFACE,
                    "ActionInvoked",
                    &(id, combined.as_str()),
                ).await;
            }
        }
    }
    Ok(())
}
