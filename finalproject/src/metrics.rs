/*
Metrics collector, we need this in order to save
the outputs from each run so that we can print the final result
after each run!
*/

use crate::task::TaskKind;
use std::time::{Duration, Instant};

/// Send records over to the done-channel.
#[derive(Clone, Debug)]
pub struct DoneRecord {
    pub id: u64,
    pub kind: TaskKind,
    pub worker_id: usize,
    pub arrival: Instant,
    pub started: Instant,
    pub finished: Instant,
}

impl DoneRecord {
    pub fn wait(&self) -> Duration {
        self.started.duration_since(self.arrival)
    }
    pub fn turnaround(&self) -> Duration {
        self.finished.duration_since(self.arrival)
    }
    pub fn service(&self) -> Duration {
        self.finished.duration_since(self.started)
    }
}

/// This goes over to the monitor every tick (the spammy printing comes from here)
#[derive(Clone, Debug, Default)]
pub struct MonitorSample {
    pub cpu_in_use: u8,
    pub active_workers: u8,
    pub ready_queue_len: usize,
}

/// Tracker for the monitor's metrics.
#[derive(Clone, Debug, Default)]
pub struct Reporter {
    pub samples: u64,
    pub cpu_total: u64,
    pub active_total: u64,
    pub queue_len_total: u64,
    pub queue_len_max: usize,
}

/// Final summary printed at the end of each sim.
#[derive(Debug)]
pub struct Summary {
    pub total_completed: usize,
    pub cpu_completed: usize,
    pub io_completed: usize,
    pub makespan: Duration,
    pub avg_wait: Duration,
    pub max_wait: Duration,
    pub avg_turnaround: Duration,
    pub avg_cpu_usage: f64,
    pub avg_active_workers: f64,
    pub worker_utilization: f64,
    pub avg_queue_len: f64,
    pub max_queue_len: usize,
}

pub fn summarize(
    records: &[DoneRecord],
    reporter: &Reporter,
    workers: usize,
    sim_start: Instant,
) -> Summary {
    let total = records.len();
    let cpu = records.iter().filter(|r| r.kind == TaskKind::Cpu).count();
    let io = total - cpu;

    let makespan = records
        .iter()
        .map(|r| r.finished)
        .max()
        .map(|f| f.duration_since(sim_start))
        .unwrap_or_default();

    let (sum_wait, max_wait, sum_turn, sum_service) = records.iter().fold(
        (
            Duration::ZERO,
            Duration::ZERO,
            Duration::ZERO,
            Duration::ZERO,
        ),
        |(sw, mw, st, ss), r| {
            let w = r.wait();
            (sw + w, mw.max(w), st + r.turnaround(), ss + r.service())
        },
    );

    let n = total.max(1) as u32;
    let avg_wait = sum_wait / n;
    let avg_turnaround = sum_turn / n;

    let samples = reporter.samples.max(1) as f64;
    let avg_cpu = reporter.cpu_total as f64 / samples;
    let avg_active = reporter.active_total as f64 / samples;
    let avg_qlen = reporter.queue_len_total as f64 / samples;

    // Utilization = total service time / (workers * makespan).
    let utilization = if makespan.is_zero() {
        0.0
    } else {
        sum_service.as_secs_f64() / (workers as f64 * makespan.as_secs_f64())
    };

    Summary {
        total_completed: total,
        cpu_completed: cpu,
        io_completed: io,
        makespan,
        avg_wait,
        max_wait,
        avg_turnaround,
        avg_cpu_usage: avg_cpu,
        avg_active_workers: avg_active,
        worker_utilization: utilization,
        avg_queue_len: avg_qlen,
        max_queue_len: reporter.queue_len_max,
    }
}

// Pretty-printing for the summary report
pub fn print_summary(label: &str, s: &Summary) {
    println!("===== {} =====", label);
    println!("Total completed:        {}", s.total_completed);
    println!("  CPU tasks:            {}", s.cpu_completed);
    println!("  IO tasks:             {}", s.io_completed);
    println!("Makespan:               {:?}", s.makespan);
    println!("Avg wait time:          {:?}", s.avg_wait);
    println!("Max wait time:          {:?}", s.max_wait);
    println!("Avg turnaround:         {:?}", s.avg_turnaround);
    println!("Avg CPU usage:          {:.1}%", s.avg_cpu_usage);
    println!(
        "Avg active workers:     {:.2}  (utilization {:.1}%)",
        s.avg_active_workers,
        s.worker_utilization * 100.0
    );
    println!(
        "Queue length:           avg {:.2}, max {}",
        s.avg_queue_len, s.max_queue_len
    );
}
