use anyhow::Result;
use serde::Serialize;
use std::sync::Arc;
use system_tray::client::{ActivateRequest, Client, Event, UpdateEvent};
use system_tray::item::{IconPixmap, StatusNotifierItem};
use system_tray::menu::{MenuItem, TrayMenu};
use tokio::sync::mpsc;
use tracing::{info, warn};

#[derive(Serialize, Clone)]
pub struct TrayMenuEntry {
    pub id: i32,
    pub label: Option<String>,
    pub enabled: bool,
    pub visible: bool,
    pub item_type: String,
    pub icon_name: Option<String>,
    pub children: Vec<TrayMenuEntry>,
}

#[derive(Serialize, Clone)]
pub struct TrayItemData {
    pub id: String,
    pub address: String,
    pub title: Option<String>,
    pub icon_name: Option<String>,
    pub icon_pixmap_path: Option<String>,
    pub has_menu: bool,
    pub menu: Vec<TrayMenuEntry>,
}

#[derive(Debug)]
pub enum TrayCommand {
    Activate { address: String, x: i32, y: i32 },
    SecondaryActivate { address: String, x: i32, y: i32 },
    Scroll { address: String, delta: i32, orientation: String },
    MenuClick { address: String, menu_id: i32 },
}

fn menu_item_to_entry(item: &MenuItem) -> TrayMenuEntry {
    TrayMenuEntry {
        id: item.id,
        label: item.label.clone(),
        enabled: item.enabled,
        visible: item.visible,
        item_type: format!("{:?}", item.menu_type),
        icon_name: item.icon_name.clone(),
        children: item.submenu.iter().map(menu_item_to_entry).collect(),
    }
}

fn menu_to_entries(menu: &TrayMenu) -> Vec<TrayMenuEntry> {
    menu.submenus.iter().map(menu_item_to_entry).collect()
}

fn pixmap_to_png(address: &str, pixmap: &IconPixmap) -> Option<String> {
    if pixmap.width <= 0 || pixmap.height <= 0 {
        return None;
    }
    let w = pixmap.width as u32;
    let h = pixmap.height as u32;
    let expected = (w * h * 4) as usize;
    if pixmap.pixels.len() < expected {
        return None;
    }
    let mut bytes = Vec::with_capacity(expected);
    for i in 0..(w * h) as usize {
        let base = i * 4;
        let a = pixmap.pixels[base];
        let r = pixmap.pixels[base + 1];
        let g = pixmap.pixels[base + 2];
        let b = pixmap.pixels[base + 3];
        bytes.push(r);
        bytes.push(g);
        bytes.push(b);
        bytes.push(a);
    }
    let buf: image::RgbaImage = image::ImageBuffer::from_raw(w, h, bytes)?;
    let safe: String = address
        .chars()
        .map(|c| if c.is_alphanumeric() { c } else { '_' })
        .collect();
    let path = format!("/tmp/mshelld-tray-{safe}.png");
    if buf.save(&path).is_ok() {
        Some(path)
    } else {
        None
    }
}

fn pick_icon_pixmap(address: &str, item: &StatusNotifierItem) -> Option<String> {
    let pixmaps = item.icon_pixmap.as_ref()?;
    let mut best: Option<&IconPixmap> = None;
    for icon in pixmaps.iter() {
        if icon.width <= 0 || icon.height <= 0 {
            continue;
        }
        match best {
            None => best = Some(icon),
            Some(b) => {
                if icon.width > b.width {
                    best = Some(icon);
                }
            }
        }
    }
    best.and_then(|pixmap| pixmap_to_png(address, pixmap))
}

fn item_to_data(address: &str, item: &StatusNotifierItem, menu: Option<&TrayMenu>) -> TrayItemData {
    let icon_name = if item.icon_name.as_deref().map_or(true, |s| s.is_empty()) {
        None
    } else {
        item.icon_name.clone()
    };
    let icon_pixmap_path = if icon_name.is_some() {
        None
    } else {
        pick_icon_pixmap(address, item)
    };
    TrayItemData {
        id: item.id.clone(),
        address: address.to_string(),
        title: item.title.clone(),
        icon_name,
        icon_pixmap_path,
        has_menu: item.menu.is_some(),
        menu: menu.map(menu_to_entries).unwrap_or_default(),
    }
}

fn cached_item(client: &Client, address: &str) -> Option<(StatusNotifierItem, Option<TrayMenu>)> {
    let map = client.items();
    let guard = map.lock().ok()?;
    guard.get(address).cloned()
}

async fn handle_command(client: &Client, cmd: TrayCommand) {
    match cmd {
        TrayCommand::Activate { address, x, y } => {
            let req = ActivateRequest::Default { address, x, y };
            if let Err(e) = client.activate(req).await {
                warn!("activate failed: {e}");
            }
        }
        TrayCommand::SecondaryActivate { address, x, y } => {
            let req = ActivateRequest::Secondary { address, x, y };
            if let Err(e) = client.activate(req).await {
                warn!("secondary activate failed: {e}");
            }
        }
        TrayCommand::MenuClick { address, menu_id } => {
            let Some((item, _)) = cached_item(client, &address) else {
                warn!("menu click: unknown address {address}");
                return;
            };
            let Some(menu_path) = item.menu.clone() else {
                warn!("menu click: no menu path for {address}");
                return;
            };
            let req = ActivateRequest::MenuItem {
                address,
                menu_path,
                submenu_id: menu_id,
            };
            if let Err(e) = client.activate(req).await {
                warn!("menu click failed: {e}");
            }
        }
        TrayCommand::Scroll { address, .. } => {
            warn!("scroll not supported by system-tray 0.8.9 (address={address})");
        }
    }
}

pub async fn run(
    client: Arc<Client>,
    tx: mpsc::UnboundedSender<String>,
    mut cmd_rx: mpsc::UnboundedReceiver<TrayCommand>,
) -> Result<()> {
    let mut events = client.subscribe();
    info!("tray client started");

    {
        let map = client.items();
        let guard = map.lock().unwrap();
        for (address, (item, menu)) in guard.iter() {
            let data = item_to_data(address, item, menu.as_ref());
            let json = serde_json::json!({
                "type": "tray",
                "event": "add",
                "item": data,
            });
            let _ = tx.send(json.to_string());
        }
    }

    loop {
        tokio::select! {
            event = events.recv() => {
                let Ok(event) = event else {
                    warn!("tray event stream ended");
                    break;
                };
                match event {
                    Event::Add(address, _item) => {
                        let Some((item, menu)) = cached_item(&client, &address) else { continue };
                        let data = item_to_data(&address, &item, menu.as_ref());
                        let json = serde_json::json!({
                            "type": "tray",
                            "event": "add",
                            "item": data,
                        });
                        let _ = tx.send(json.to_string());
                    }
                    Event::Update(address, update) => {
                        match update {
                            UpdateEvent::Menu(menu) => {
                                let entries = menu_to_entries(&menu);
                                let json = serde_json::json!({
                                    "type": "tray",
                                    "event": "menu",
                                    "address": address,
                                    "menu": entries,
                                });
                                let _ = tx.send(json.to_string());
                            }
                            _ => {
                                if let Some((item, menu)) = cached_item(&client, &address) {
                                    let data = item_to_data(&address, &item, menu.as_ref());
                                    let json = serde_json::json!({
                                        "type": "tray",
                                        "event": "update",
                                        "item": data,
                                    });
                                    let _ = tx.send(json.to_string());
                                }
                            }
                        }
                    }
                    Event::Remove(address) => {
                        let json = serde_json::json!({
                            "type": "tray",
                            "event": "remove",
                            "address": address,
                        });
                        let _ = tx.send(json.to_string());
                    }
                }
            }
            cmd = cmd_rx.recv() => {
                match cmd {
                    Some(c) => handle_command(&client, c).await,
                    None => break,
                }
            }
        }
    }
    Ok(())
}
