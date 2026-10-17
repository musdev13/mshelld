use anyhow::Result;
use std::path::PathBuf;
use tokio::sync::mpsc;
use tokio::time::{interval, Duration};

fn find_battery() -> Option<PathBuf> {
    let dir = std::fs::read_dir("/sys/class/power_supply").ok()?;
    for entry in dir.flatten() {
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if name.starts_with("BAT") {
            return Some(entry.path());
        }
    }
    None
}

fn read_trim(path: &PathBuf) -> Option<String> {
    std::fs::read_to_string(path).ok().map(|s| s.trim().to_string())
}

pub async fn run(tx: mpsc::UnboundedSender<String>) -> Result<()> {
    let mut tick = interval(Duration::from_secs(2));
    let mut last_state: Option<(bool, Option<f32>, Option<String>)> = None;

    loop {
        tick.tick().await;

        let Some(bat) = find_battery() else {
            let current = (false, None, None);
            if last_state.as_ref() == Some(&current) {
                continue;
            }
            last_state = Some(current);
            let json = serde_json::json!({
                "type": "battery",
                "present": false,
            });
            let _ = tx.send(json.to_string());
            continue;
        };

        let capacity = read_trim(&bat.join("capacity")).and_then(|s| s.parse::<f32>().ok());
        let status = read_trim(&bat.join("status"));
        let percentage = capacity.map(|c| c / 100.0);

        let current = (true, percentage, status.clone());
        if last_state.as_ref() == Some(&current) {
            continue;
        }
        last_state = Some(current);

        let json = serde_json::json!({
            "type": "battery",
            "present": true,
            "percentage": percentage,
            "state": status,
        });
        let _ = tx.send(json.to_string());
    }
}
