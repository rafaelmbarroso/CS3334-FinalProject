/*
Dispacher thread: This spawns our threads that generatates the total_tasks then sends
them over to the manager thread. In this implementation a fixed seed is used for debugging purposes.
When there's no more tasks to generate the channel shuts down and the manager swill then see there's no
more tasks to receive.
*/

use crate::config::Params;
use crate::manager::Event;
use crate::task::{Task, TaskKind};
use rand::prelude::*;
use rand::rngs::StdRng;
use std::sync::mpsc::Sender;
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

pub fn spawn(params: &Params, tx: Sender<Event>) -> JoinHandle<()> {
    let total = params.total_tasks;
    let interval = Duration::from_millis(params.arrival_interval_ms);
    let duration = Duration::from_millis(params.task_duration_ms);
    let io_prob = params.io_probability;
    let seed = params.rng_seed;

    thread::spawn(move || {
        let mut rng = StdRng::seed_from_u64(seed);
        for id in 0..total {
            let kind = if rng.random_bool(io_prob) {
                TaskKind::Io
            } else {
                TaskKind::Cpu
            };
            let task = Task::new(id, kind, duration, Instant::now());

            // If the manager has hung up, shutdown is in progress. Else just stop early.
            if tx.send(Event::Arrived(task)).is_err() {
                return;
            }

            thread::sleep(interval);
        }
        // Sender drops here, manager will see the channel close after draining.
    })
}
