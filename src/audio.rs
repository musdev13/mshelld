use anyhow::Result;
use std::process::Stdio;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Command;
use tokio::sync::mpsc;

#[derive(Debug)]
pub enum AudioCommand {
    ToggleSinkMute,
    ToggleSourceMute,
}

async fn pactl(args: &[&str]) -> Result<String> {
    let out = Command::new("pactl").args(args).output().await?;
    Ok(String::from_utf8_lossy(&out.stdout).trim().to_string())
}

fn parse_percent(s: &str) -> f32 {
    for part in s.split_whitespace() {
        if let Some(pct) = part.strip_suffix('%') {
            if let Ok(v) = pct.parse::<f32>() {
                return v / 100.0;
            }
        }
    }
    0.0
}

async fn emit_audio(tx: &mpsc::UnboundedSender<String>) {
    let sink_vol_s = pactl(&["get-sink-volume", "@DEFAULT_SINK@"]).await.unwrap_or_default();
    let sink_mute_s = pactl(&["get-sink-mute", "@DEFAULT_SINK@"]).await.unwrap_or_default();
    let sink_port = pactl(&["get-sink-port", "@DEFAULT_SINK@"]).await.unwrap_or_default();
    let src_vol_s = pactl(&["get-source-volume", "@DEFAULT_SOURCE@"]).await.unwrap_or_default();
    let src_mute_s = pactl(&["get-source-mute", "@DEFAULT_SOURCE@"]).await.unwrap_or_default();

    let sink_volume = parse_percent(&sink_vol_s);
    let sink_muted = sink_mute_s.to_lowercase().contains("yes");
    let device = if sink_port.contains("headphone") { "headphones" } else { "speakers" };
    let source_volume = parse_percent(&src_vol_s);
    let source_muted = src_mute_s.to_lowercase().contains("yes");

    let json = serde_json::json!({
        "type": "audio",
        "sink": { "volume": sink_volume, "muted": sink_muted, "device": device },
        "source": { "volume": source_volume, "muted": source_muted },
    });
    let _ = tx.send(json.to_string());
}

async fn handle_command(tx: &mpsc::UnboundedSender<String>, cmd: AudioCommand) {
    match cmd {
        AudioCommand::ToggleSinkMute => {
            let _ = pactl(&["set-sink-mute", "@DEFAULT_SINK@", "toggle"]).await;
        }
        AudioCommand::ToggleSourceMute => {
            let _ = pactl(&["set-source-mute", "@DEFAULT_SOURCE@", "toggle"]).await;
        }
    }
    emit_audio(tx).await;
}

pub async fn run(
    tx: mpsc::UnboundedSender<String>,
    mut cmd_rx: mpsc::UnboundedReceiver<AudioCommand>,
) -> Result<()> {
    emit_audio(&tx).await;

    let mut child = Command::new("pactl")
        .arg("subscribe")
        .stdout(Stdio::piped())
        .spawn()?;
    let stdout = child.stdout.take().ok_or_else(|| anyhow::anyhow!("pactl no stdout"))?;
    let mut reader = BufReader::new(stdout).lines();

    loop {
        tokio::select! {
            line = reader.next_line() => {
                match line {
                    Ok(Some(line)) => {
                        if line.contains("on sink #")
                            || line.contains("on source #")
                            || line.contains("on server #")
                            || line.contains("on sink-input #")
                        {
                            emit_audio(&tx).await;
                        }
                    }
                    _ => break,
                }
            }
            cmd = cmd_rx.recv() => {
                match cmd {
                    Some(c) => handle_command(&tx, c).await,
                    None => break,
                }
            }
        }
    }
    Ok(())
}
