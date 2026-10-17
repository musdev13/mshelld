use anyhow::Result;
use serde_json::Value;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::{broadcast, mpsc, Mutex};
use tracing::{info, warn};

use crate::audio::AudioCommand;
use crate::notifications::NotificationCommand;
use crate::tray::TrayCommand;

pub fn socket_path() -> PathBuf {
    let base = std::env::var("XDG_RUNTIME_DIR").unwrap_or_else(|_| "/tmp".to_string());
    PathBuf::from(base).join("mshelld.sock")
}

#[derive(Default)]
pub struct Cache {
    pub tray: HashMap<String, Value>,
    pub audio: Option<Value>,
    pub battery: Option<Value>,
    pub notifications: HashMap<u64, Value>,
}

impl Cache {
    fn update(&mut self, msg: &Value) {
        let kind = msg.get("type").and_then(|v| v.as_str()).unwrap_or("");
        match kind {
            "tray" => {
                let event = msg.get("event").and_then(|v| v.as_str()).unwrap_or("");
                match event {
                    "add" | "update" => {
                        if let Some(item) = msg.get("item") {
                            if let Some(addr) = item.get("address").and_then(|v| v.as_str()) {
                                self.tray.insert(addr.to_string(), item.clone());
                            }
                        }
                    }
                    "menu" => {
                        if let Some(addr) = msg.get("address").and_then(|v| v.as_str()) {
                            if let Some(existing) = self.tray.get_mut(addr) {
                                let mut new_item = existing.clone();
                                if let Some(menu) = msg.get("menu") {
                                    new_item["menu"] = menu.clone();
                                    new_item["has_menu"] = Value::Bool(
                                        menu.as_array().map(|a| !a.is_empty()).unwrap_or(false)
                                    );
                                }
                                *existing = new_item;
                            }
                        }
                    }
                    "remove" => {
                        if let Some(addr) = msg.get("address").and_then(|v| v.as_str()) {
                            self.tray.remove(addr);
                        }
                    }
                    _ => {}
                }
            }
            "audio" => {
                self.audio = Some(msg.clone());
            }
            "battery" => {
                self.battery = Some(msg.clone());
            }
            "notification" => {
                let event = msg.get("event").and_then(|v| v.as_str()).unwrap_or("");
                match event {
                    "add" | "update" => {
                        if let Some(n) = msg.get("notification") {
                            if let Some(id) = n.get("id").and_then(|v| v.as_u64()) {
                                self.notifications.insert(id, n.clone());
                            }
                        }
                    }
                    "close" => {
                        if let Some(id) = msg.get("id").and_then(|v| v.as_u64()) {
                            self.notifications.remove(&id);
                        }
                    }
                    _ => {}
                }
            }
            _ => {}
        }
    }

    fn replay(&self) -> Vec<String> {
        let mut out = Vec::new();
        for (addr, item) in self.tray.iter() {
            let json = serde_json::json!({
                "type": "tray",
                "event": "add",
                "item": item,
            });
            let _ = addr;
            out.push(json.to_string());
        }
        if let Some(a) = &self.audio {
            out.push(a.to_string());
        }
        if let Some(b) = &self.battery {
            out.push(b.to_string());
        }
        for (_id, n) in self.notifications.iter() {
            let json = serde_json::json!({
                "type": "notification",
                "event": "add",
                "notification": n,
            });
            out.push(json.to_string());
        }
        out
    }
}

pub async fn bridge(
    mut rx: mpsc::UnboundedReceiver<String>,
    out_tx: broadcast::Sender<String>,
    cache: Arc<Mutex<Cache>>,
) {
    while let Some(line) = rx.recv().await {
        if let Ok(val) = serde_json::from_str::<Value>(&line) {
            let mut c = cache.lock().await;
            c.update(&val);
        }
        let _ = out_tx.send(line);
    }
}

async fn handle_client(
    stream: UnixStream,
    out_tx: broadcast::Sender<String>,
    cache: Arc<Mutex<Cache>>,
    tray_tx: mpsc::UnboundedSender<TrayCommand>,
    audio_tx: mpsc::UnboundedSender<AudioCommand>,
    notif_tx: mpsc::UnboundedSender<NotificationCommand>,
) -> Result<()> {
    let (read_half, write_half) = stream.into_split();
    let write_half = Arc::new(Mutex::new(write_half));
    let mut reader = BufReader::new(read_half).lines();
    let mut out_rx = out_tx.subscribe();

    {
        let snapshot = {
            let c = cache.lock().await;
            c.replay()
        };
        let mut w = write_half.lock().await;
        for line in snapshot {
            let mut buf = Vec::with_capacity(line.len() + 1);
            buf.extend_from_slice(line.as_bytes());
            buf.push(b'\n');
            if w.write_all(&buf).await.is_err() {
                return Ok(());
            }
        }
    }

    loop {
        tokio::select! {
            line = reader.next_line() => {
                let Ok(Some(line)) = line else { break };
                let trimmed = line.trim();
                if trimmed.is_empty() { continue; }
                let val: Value = match serde_json::from_str(trimmed) {
                    Ok(v) => v,
                    Err(e) => { warn!("bad json: {e}"); continue; }
                };
                let cmd = val.get("cmd").and_then(|v| v.as_str()).unwrap_or("");
                match cmd {
                    "tray.activate" => {
                        let address = val.get("address").and_then(|v| v.as_str()).unwrap_or("").to_string();
                        let x = val.get("x").and_then(|v| v.as_i64()).unwrap_or(0) as i32;
                        let y = val.get("y").and_then(|v| v.as_i64()).unwrap_or(0) as i32;
                        let _ = tray_tx.send(TrayCommand::Activate { address, x, y });
                    }
                    "tray.secondaryActivate" => {
                        let address = val.get("address").and_then(|v| v.as_str()).unwrap_or("").to_string();
                        let x = val.get("x").and_then(|v| v.as_i64()).unwrap_or(0) as i32;
                        let y = val.get("y").and_then(|v| v.as_i64()).unwrap_or(0) as i32;
                        let _ = tray_tx.send(TrayCommand::SecondaryActivate { address, x, y });
                    }
                    "tray.menuClick" => {
                        let address = val.get("address").and_then(|v| v.as_str()).unwrap_or("").to_string();
                        let menu_id = val.get("menu_id").and_then(|v| v.as_i64()).unwrap_or(0) as i32;
                        let _ = tray_tx.send(TrayCommand::MenuClick { address, menu_id });
                    }
                    "audio.toggleSinkMute" => { let _ = audio_tx.send(AudioCommand::ToggleSinkMute); }
                    "audio.toggleSourceMute" => { let _ = audio_tx.send(AudioCommand::ToggleSourceMute); }
                    "notification.dismiss" => {
                        let id = val.get("id").and_then(|v| v.as_u64()).unwrap_or(0) as u32;
                        let _ = notif_tx.send(NotificationCommand::Dismiss { id });
                    }
                    "notification.invokeAction" => {
                        let id = val.get("id").and_then(|v| v.as_u64()).unwrap_or(0) as u32;
                        let key = val.get("key").and_then(|v| v.as_str()).unwrap_or("").to_string();
                        let _ = notif_tx.send(NotificationCommand::InvokeAction { id, key });
                    }
                    "notification.reply" => {
                        let id = val.get("id").and_then(|v| v.as_u64()).unwrap_or(0) as u32;
                        let text = val.get("text").and_then(|v| v.as_str()).unwrap_or("").to_string();
                        let _ = notif_tx.send(NotificationCommand::Reply { id, text });
                    }
                    _ => warn!("unknown cmd: {cmd}"),
                }
            }
            msg = out_rx.recv() => {
                let Ok(msg) = msg else { break };
                let mut w = write_half.lock().await;
                let mut buf = Vec::with_capacity(msg.len() + 1);
                buf.extend_from_slice(msg.as_bytes());
                buf.push(b'\n');
                if w.write_all(&buf).await.is_err() {
                    break;
                }
            }
        }
    }
    Ok(())
}

pub async fn run(
    out_tx: broadcast::Sender<String>,
    cache: Arc<Mutex<Cache>>,
    tray_tx: mpsc::UnboundedSender<TrayCommand>,
    audio_tx: mpsc::UnboundedSender<AudioCommand>,
    notif_tx: mpsc::UnboundedSender<NotificationCommand>,
) -> Result<()> {
    let path = socket_path();
    if path.exists() {
        let _ = std::fs::remove_file(&path);
    }
    let listener = UnixListener::bind(&path)?;
    info!("ipc listening on {}", path.display());

    loop {
        let (stream, _) = listener.accept().await?;
        let out_tx = out_tx.clone();
        let cache = cache.clone();
        let tray_tx = tray_tx.clone();
        let audio_tx = audio_tx.clone();
        let notif_tx = notif_tx.clone();
        tokio::spawn(async move {
            if let Err(e) = handle_client(stream, out_tx, cache, tray_tx, audio_tx, notif_tx).await {
                warn!("client error: {e:#}");
            }
        });
    }
}
