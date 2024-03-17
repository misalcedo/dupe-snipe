use std::collections::HashSet;
use std::fs::FileType;
use std::future::Future;
use std::hash::{Hash, Hasher};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use clap::Parser;
use log::{debug, error};
use sha3::Digest;
use tokio::sync::mpsc::{Receiver, UnboundedSender};
use tokio::sync::mpsc::error::SendError;
use tokio::task::JoinSet;

#[derive(clap::Parser)]
#[command(author, version, about, long_about)]
struct Options {
    /// The directory to check for duplicates in.
    #[arg(short, long)]
    input: PathBuf,
    /// The directory to move all unique files to.
    #[arg(short, long)]
    output: PathBuf,
    /// The maximum number of files to load into memory concurrently. Defaults to the number of CPU cores.
    #[arg(short, long, default_value_t = 0)]
    concurrency: usize
}

#[derive(Eq)]
struct FileDigest {
    path: PathBuf,
    digest: String,
    size: usize,
}

impl PartialEq for FileDigest {
    fn eq(&self, other: &Self) -> bool {
        self.digest.eq(&other.digest) && self.size == other.size
    }
}

impl Hash for FileDigest {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.digest.hash(state);
        self.size.hash(state);
    }
}

#[derive(Debug)]
struct FileError {
    path: PathBuf,
    error: io::Error,
}

#[tokio::main]
async fn main() {
    env_logger::init();

    let mut options = Options::parse();
    let mut workers = JoinSet::new();

    if options.concurrency == 0 {
        options.concurrency = num_cpus::get();
    }

    let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
    let mut queues = Vec::with_capacity(options.concurrency);

    for _ in 0..options.concurrency {
        let (queue_sender, queue_receiver) = tokio::sync::mpsc::channel(1);

        queues.push(queue_sender);
        workers.spawn(work(queue_receiver, sender.clone()));
    }

    let mut files = 0;
    let mut directories = vec![options.input];
    while let Some(directory) = directories.pop() {
        debug!("listing children of {}", directory.display());

        let mut children = tokio::fs::read_dir(&directory).await.expect("unable to read input directory");
        while let Some(child) = children.next_entry().await.expect("unable to iterate child directories") {
            match child.metadata().await {
                Ok(metadata) => {
                    if metadata.is_dir() {
                        directories.push(child.path());
                    } else {
                        files += 1;

                        let mut pending = true;
                        while pending {
                            for queue in queues {
                                if let Ok(_) = queue.try_send(child.path()) {
                                    pending = false;
                                    break;
                                }
                            }
                        }
                    }
                }
                Err(e) => error!("unable to read metadata for child directory {}: {e}", child.path().display())
            }
        }
    }

    queues.drain(..);

    while let Some(result) = workers.join_next().await {
        if let Err(e) = result {
            error!("worker unable to send results: {e}")
        }
    }

    drop(sender);

    let mut originals = HashSet::with_capacity(files);
    while let Some(result) = receiver.recv().await {
        match result {
            Ok(digest) => {
                debug!("checking {} for uniqueness...", digest.path.display());
                if !originals.insert(digest) {
                    debug!("found duplicate file");
                }
            }
            Err(e) => error!("unable to process {}: {}", e.path.display(), e.error)
        }
    }

    let mut movers = JoinSet::new();
    let output = Arc::new(options.output);

    for digest in originals.drain() {
        movers.spawn(move_file(digest.path, Arc::clone(&output)))
    }

    while let Some(result) = movers.join_next().await {
        if let Err(e) = result {
            error!("unable to move file: {e}")
        }
    }
}

async fn work(mut receiver: Receiver<PathBuf>, mut sender: UnboundedSender<Result<FileDigest, FileError>>) -> Result<(), SendError<io::Result<FileDigest>>> {
    while let Some(path) = receiver.recv().await {
        match tokio::fs::read(&path).await {
            Ok(contents) => {
                let mut hasher = sha3::Sha3_256::new();

                hasher.update(&contents);

                let digest = hex::encode(hasher.finalize());

                sender.send(Ok(FileDigest {
                    path,
                    digest,
                    size: contents.len(),
                }))?;
            }
            Err(e) => sender.send(Err(FileError {
                path,
                error: e
            }))?
        }
    }

    Ok(())
}

async fn move_file(old_path: PathBuf, target: Arc<PathBuf>) -> Result<(), FileError> {
    debug!("moving file {}", old_path.display());

    let new_path = target.join(old_path.file_name().expect("expected a file"));

    debug!("moving file {} to {}", old_path.display(), new_path.display());

    Ok(())
}
