//! Entry point — wires together generator, manager, workers, and monitor.
//!
//! Thread layout (matches the amendments page = 11 threads):
//!   1 main + 1 generator + 1 manager + 8 workers + 1 monitor
//!
//! Channels:
//!   generator --Event::Arrived--> manager
//!   workers   --Event::WorkerReady / Freed--> manager
//!   manager   --Task--> worker[i]   (one channel per worker)
//!   workers   --DoneRecord--> main  (collected after join for metrics)
//!   monitor reads atomics published by manager (MgrSnapshot)
//!
//! Shutdown: manager counts Freed events; once it sees all `total_tasks`
//! completions it drops the per-worker senders, workers exit, main joins
//! everything, then flips the monitor's shutdown flag.

mod config;
mod generator;
mod manager;
mod metrics;
mod monitor;
mod task;
mod worker;

use crate::config::{Params, Policy};
use crate::manager::{Event, MgrSnapshot};
use crate::metrics::{DoneRecord, print_summary, summarize};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::time::Instant;

fn run_experiment(label: &str, params: Params) {
    println!("\n############ {} ############", label);
    println!("{:#?}", params);

    let sim_start = Instant::now();

    // Channels
    let (event_tx, event_rx) = mpsc::channel::<Event>();

    let mut worker_task_txs = Vec::with_capacity(params.workers);
    let mut worker_task_rxs = Vec::with_capacity(params.workers);
    for _ in 0..params.workers {
        let (tx, rx) = mpsc::channel::<crate::task::Task>();
        worker_task_txs.push(tx);
        worker_task_rxs.push(rx);
    }

    let (done_tx, done_rx) = mpsc::channel::<DoneRecord>();

    // The shared snapshot for both our monitor and manager
    let snapshot = MgrSnapshot::new();
    let shutdown = Arc::new(AtomicBool::new(false));

    // Spawn workers
    let mut worker_handles = Vec::with_capacity(params.workers);
    for (id, rx) in worker_task_rxs.into_iter().enumerate() {
        worker_handles.push(worker::spawn(id, rx, event_tx.clone(), done_tx.clone()));
    }

    // Spawn manager
    let manager_handle = manager::spawn(&params, event_rx, worker_task_txs, snapshot.clone());

    // Spawn monitor
    let monitor_handle = monitor::spawn(&params, snapshot.clone(), shutdown.clone(), sim_start);

    // Spawn generator (prep the workers and managers)
    let generator_handle = generator::spawn(&params, event_tx.clone());

    // Start the cleanup process, drop the senders to signal shutdown once the generator and manager are done.
    drop(event_tx);
    drop(done_tx);

    // Wait for the pipeline to drain
    generator_handle.join().expect("generator panicked");
    manager_handle.join().expect("manager panicked");
    for h in worker_handles {
        h.join().expect("worker panicked");
    }

    // Now that all workers are gone, stop the monitor.
    shutdown.store(true, Ordering::Relaxed);
    let report = monitor_handle.join().expect("monitor panicked");

    // Collect DoneRecords
    // done_tx was cloned per worker; all clones dropped by now, so the
    // channel is closed and `iter()` will terminate.
    let records: Vec<DoneRecord> = done_rx.iter().collect();

    let summary = summarize(&records, &report, params.workers, sim_start);
    print_summary(label, &summary);
}

fn main() {
    // Sim A: balanced workload, FIFO baseline.
    run_experiment("Experiment A — Balanced / FIFO", Params::balanced(Policy::Fifo));

    // Sim B: stressed workload, FIFO (shows where FIFO struggles).
    run_experiment(
        "Experiment B — Stressed / FIFO",
        Params::stressed(Policy::Fifo),
    );

    // Sim C: same stressed workload, Optimize policy (the comparison).
    run_experiment(
        "Experiment C — Stressed / Optimize",
        Params::stressed(Policy::Optimize),
    );
}
