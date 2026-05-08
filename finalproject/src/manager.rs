/*
Manager thread: Delegates tasks to workers while adhering to the CPU cap (100%)
It'll stop once it stops seeing new tasks from the dispatcher and all tasks have finished.
*/

use crate::config::{Params, Policy};
use crate::task::{Task, TaskKind};
use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, AtomicUsize, Ordering};
use std::sync::mpsc::{Receiver, Sender};
use std::thread::{self, JoinHandle};

/// Everything the manager listens to lands as one of these.
pub enum Event {
    /// Generator handed us a freshly-created task.
    Arrived(Task),
    /// A worker came online and is ready for its first task.
    WorkerReady { worker_id: usize },
    /// A worker just finished a task. Free its CPU and mark it idle.
    Freed { worker_id: usize, cpu_cost: u8 },
}

/// Read only view that the monitor thread checks on every tick.
#[derive(Clone)]
pub struct MgrSnapshot {
    pub cpu_in_use: Arc<AtomicU8>,
    pub active_workers: Arc<AtomicU8>,
    pub queue_len: Arc<AtomicUsize>,
}

impl MgrSnapshot {
    pub fn new() -> Self {
        Self {
            cpu_in_use: Arc::new(AtomicU8::new(0)),
            active_workers: Arc::new(AtomicU8::new(0)),
            queue_len: Arc::new(AtomicUsize::new(0)),
        }
    }
}

pub fn spawn(
    params: &Params,
    rx: Receiver<Event>,
    worker_txs: Vec<Sender<Task>>,
    snapshot: MgrSnapshot,
) -> JoinHandle<()> {
    let workers = params.workers;
    let budget = params.cpu_budget;
    let policy = params.policy;
    let total_tasks = params.total_tasks;

    thread::spawn(move || {
        // Close up shop once all the tasks are depleated.
        let mut tasks_completed: u64 = 0;
        // This is mainly so that the packed policy can look at the queues 
        // seperately, but FIFO grabs one then the other so both require this separation.
        let mut cpu_q: VecDeque<Task> = VecDeque::new();
        let mut io_q: VecDeque<Task> = VecDeque::new();
        // For pure FIFO we need global insertion order:
        let mut fifo_q: VecDeque<Task> = VecDeque::new();

        let mut idle_workers: VecDeque<usize> = VecDeque::with_capacity(workers);
        let mut cpu_in_use: u8 = 0;

        for ev in rx.iter() {
            match ev {
                Event::Arrived(task) => match policy {
                    Policy::Fifo => fifo_q.push_back(task),
                    Policy::Optimize => match task.kind {
                        TaskKind::Cpu => cpu_q.push_back(task),
                        TaskKind::Io => io_q.push_back(task),
                    },
                },
                Event::WorkerReady { worker_id } => {
                    idle_workers.push_back(worker_id);
                }
                Event::Freed {
                    worker_id,
                    cpu_cost,
                } => {
                    cpu_in_use = cpu_in_use.saturating_sub(cpu_cost);
                    idle_workers.push_back(worker_id);
                    snapshot
                        .active_workers
                        .fetch_sub(1, Ordering::Relaxed);
                    tasks_completed += 1;
                }
            }

            // Try to dispatch as many tasks as the budget + idle pool allow.
            dispatch_loop(
                policy,
                &mut fifo_q,
                &mut cpu_q,
                &mut io_q,
                &mut idle_workers,
                &mut cpu_in_use,
                budget,
                &worker_txs,
                &snapshot,
            );

            // Republish queue-length snapshot for the monitor (spammy print).
            let qlen = fifo_q.len() + cpu_q.len() + io_q.len();
            snapshot.queue_len.store(qlen, Ordering::Relaxed);
            snapshot.cpu_in_use.store(cpu_in_use, Ordering::Relaxed);

            // Part of the shutdown procedure, close the channel once all tasks are done.
            if tasks_completed >= total_tasks {
                break;
            }
        }

        // Drop worker senders -> since the workers end up with err when the channel closes,
        // they will exit cleanly rather than crash the program.
        let _ = workers;
        drop(worker_txs);
    })
}

#[allow(clippy::too_many_arguments)]
fn dispatch_loop(
    policy: Policy,
    fifo_q: &mut VecDeque<Task>,
    cpu_q: &mut VecDeque<Task>,
    io_q: &mut VecDeque<Task>,
    idle: &mut VecDeque<usize>,
    cpu_in_use: &mut u8,
    budget: u8,
    worker_txs: &[Sender<Task>],
    snapshot: &MgrSnapshot,
) {
    loop {
        if idle.is_empty() {
            return;
        }

        let next: Option<Task> = match policy {
            Policy::Fifo => pick_fifo(fifo_q, *cpu_in_use, budget),
            Policy::Optimize => pick_optimize(cpu_q, io_q, *cpu_in_use, budget),
        };

        let Some(task) = next else { return };

        let worker_id = idle.pop_front().expect("checked above");
        *cpu_in_use += task.cpu_cost;
        snapshot.cpu_in_use.store(*cpu_in_use, Ordering::Relaxed);
        snapshot.active_workers.fetch_add(1, Ordering::Relaxed);

        if worker_txs[worker_id].send(task).is_err() {
            // Dead worker insurance.
            return;
        }
    }
}

/// Don't let FIFO overfit the CPU budget, that means even if we're 
/// leaving the CPU idle. We need to keep the CPU cap in mind at all times
fn pick_fifo(q: &mut VecDeque<Task>, cpu_in_use: u8, budget: u8) -> Option<Task> {
    let front = q.front()?;
    if cpu_in_use + front.cpu_cost <= budget {
        q.pop_front()
    } else {
        None
    }
}

/// Optimize: greedily fill remaining CPU headroom.
/// Pack CPU + IO tasks together so we max out our CPU util
fn pick_optimize(
    cpu_q: &mut VecDeque<Task>,
    io_q: &mut VecDeque<Task>,
    cpu_in_use: u8,
    budget: u8,
) -> Option<Task> {
    if let Some(t) = cpu_q.front() {
        if cpu_in_use + t.cpu_cost <= budget {
            return cpu_q.pop_front();
        }
    }
    if let Some(t) = io_q.front() {
        if cpu_in_use + t.cpu_cost <= budget {
            return io_q.pop_front();
        }
    }
    None
}
