//! Dropping a Directory::query stream leaves its background task running forever.
//!
//! Start the test server with `docker compose up -d tests`, then run
//! `RUST_LOG=debug cargo run -p smb --example query_dir_leak`.
use futures_util::StreamExt;
use smb::*;
use std::{sync::Arc, time::Duration};

const NUM_FILES: usize = 64;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    env_logger::init();
    let client = Client::new(ClientConfig::default());
    let share = UncPath::new("127.0.0.1")?.with_share("MyShare")?;
    client
        .share_connect(&share, "LocalAdmin", "123456".to_string())
        .await?;

    // A fresh directory per run, so reruns need no cleanup.
    let dir_name = format!(
        "query_leak_repro_{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis()
    );
    let dir_path = share.clone().with_path(&dir_name);
    client
        .create_file(
            &dir_path,
            &FileCreateArgs::make_create_new(
                FileAttributes::new().with_directory(true),
                CreateOptions::new().with_directory_file(true),
            ),
        )
        .await?
        .unwrap_dir()
        .close()
        .await?;
    for i in 0..NUM_FILES {
        client
            .create_file(
                &share.clone().with_path(&format!("{dir_name}\\file_{i}")),
                &FileCreateArgs::make_create_new(Default::default(), Default::default()),
            )
            .await?
            .unwrap_file()
            .close()
            .await?;
    }

    let dir = Arc::new(
        client
            .create_file(
                &dir_path,
                &FileCreateArgs::make_open_existing(
                    DirAccessMask::new()
                        .with_list_directory(true)
                        .with_synchronize(true)
                        .into(),
                ),
            )
            .await?
            .unwrap_dir(),
    );

    {
        // The default buffer fits all entries in one batch, so taking one item leaves the
        // rest queued and does not request another batch. Give the background task time
        // to finish sending the batch and start waiting for that request.
        let mut stream = Directory::query::<FileDirectoryInformation>(&dir, "file_*").await?;
        stream.next().await.unwrap()?;
        tokio::time::sleep(Duration::from_secs(1)).await;
    } // stream dropped here

    tokio::time::sleep(Duration::from_secs(5)).await;
    // The stream's background task holds a clone of `dir`. Once it exits, only ours remains.
    assert_eq!(
        Arc::strong_count(&dir),
        1,
        "background task still holds the directory"
    );
    println!("no leak");
    Ok(())
}
