mod audio;
mod battery;
mod ipc;
mod notifications;
mod tray;

use anyhow::Result;
use std::sync::Arc;
use system_tray::client::Client;
use tokio::sync::{broadcast, mpsc, Mutex};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_max_level(tracing::Level::INFO)
        .init();

    let (out_tx, _) = broadcast::channel::<String>(512);
    let (line_tx, line_rx) = mpsc::unbounded_channel::<String>();
    let (tray_cmd_tx, tray_cmd_rx) = mpsc::unbounded_channel::<tray::TrayCommand>();
    let (audio_cmd_tx, audio_cmd_rx) = mpsc::unbounded_channel::<audio::AudioCommand>();
    let (notif_cmd_tx, notif_cmd_rx) = mpsc::unbounded_channel::<notifications::NotificationCommand>();

    let cache = Arc::new(Mutex::new(ipc::Cache::default()));

    let client = Arc::new(Client::new().await?);

    let client_tray = client.clone();
    let line_tray = line_tx.clone();
    tokio::spawn(async move {
        if let Err(e) = tray::run(client_tray, line_tray, tray_cmd_rx).await {
            tracing::error!("tray error: {e:#}");
        }
    });

    let line_audio = line_tx.clone();
    tokio::spawn(async move {
        if let Err(e) = audio::run(line_audio, audio_cmd_rx).await {
            tracing::error!("audio error: {e:#}");
        }
    });

    let line_battery = line_tx.clone();
    tokio::spawn(async move {
        if let Err(e) = battery::run(line_battery).await {
            tracing::error!("battery error: {e:#}");
        }
    });

    let line_notif = line_tx.clone();
    let conn = zbus::Connection::session().await?;
    let server = notifications::NotificationsServer::new(line_notif);
    let state = server.state_handle();
    conn.object_server()
        .at("/org/freedesktop/Notifications", server)
        .await?;
    conn.request_name("org.freedesktop.Notifications").await?;

    let conn_notif = conn.clone();
    let line_notif_run = line_tx.clone();
    tokio::spawn(async move {
        if let Err(e) = notifications::run(conn_notif, state, line_notif_run, notif_cmd_rx).await {
            tracing::error!("notifications error: {e:#}");
        }
    });

    let bridge_tx = out_tx.clone();
    let bridge_cache = cache.clone();
    tokio::spawn(async move {
        ipc::bridge(line_rx, bridge_tx, bridge_cache).await;
    });

    let ipc_tx = out_tx.clone();
    let ipc_cache = cache.clone();
    tokio::spawn(async move {
        if let Err(e) = ipc::run(ipc_tx, ipc_cache, tray_cmd_tx, audio_cmd_tx, notif_cmd_tx).await {
            tracing::error!("ipc error: {e:#}");
        }
    });

    drop(line_tx);

    tokio::signal::ctrl_c().await?;
    Ok(())
}
