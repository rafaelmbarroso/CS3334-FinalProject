/*
Worker pool. 8 dumb consumers: receive an assigned task, sleep for its
duration, report completion + free signal back to the manager.
Very simple.
*/

use crate::manager::Event;
use crate::metrics::DoneRecord;
use crate::task::Task;
use std::sync::mpsc::{Receiver, Sender};
use std::thread::{self, JoinHandle};
use std::time::Instant;

/// Spawn the worker thread.
/// It'll do this for every task it receives until the manager closes up shop (channel closed).
pub fn spawn(
    worker_id: usize,
    task_rx: Receiver<Task>,
    mgr_tx: Sender<Event>,
    done_tx: Sender<DoneRecord>,
) -> JoinHandle<()> {
    thread::spawn(move || {
        // Tell the manager we're idle and ready for our first task.
        let _ = mgr_tx.send(Event::WorkerReady { worker_id });

        while let Ok(task) = task_rx.recv() {
            let started = Instant::now();
            let cpu_cost = task.cpu_cost;
            let kind = task.kind;
            let id = task.id;
            let arrival = task.arrival_time;

            // Simulate the work (this is where our IO and CPU both sleep 200ms)
            thread::sleep(task.duration);

            let finished = Instant::now();

            let _ = done_tx.send(DoneRecord {
                id,
                kind,
                worker_id,
                arrival,
                started,
                finished,
            });

            // Tell the manager "I'm free again" to free CPU slots and maybe get a new task
            if mgr_tx
                .send(Event::Freed {
                    worker_id,
                    cpu_cost,
                })
                .is_err()
            {
                break;
            }
        }
        // task_rx closed -> manager has shut us down.
    })
}
