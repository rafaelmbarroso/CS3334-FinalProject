/*
Monitor Thread: poll the manager how it's doing and 
log on every tick. This is where the spammy printing comes from.
*/
use crate::config::Params;
use crate::manager::MgrSnapshot;
use crate::metrics::Reporter;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

pub fn spawn(
    params: &Params,
    snapshot: MgrSnapshot,
    shutdown: Arc<AtomicBool>,
    sim_start: Instant,
) -> JoinHandle<Reporter> {
    let tick = Duration::from_millis(params.monitor_tick_ms);
    let total_tasks = params.total_tasks;
    let workers = params.workers;

    thread::spawn(move || {
        let mut report = Reporter::default();

        while !shutdown.load(Ordering::Relaxed) {
            let cpu = snapshot.cpu_in_use.load(Ordering::Relaxed);
            let active = snapshot.active_workers.load(Ordering::Relaxed);
            let qlen = snapshot.queue_len.load(Ordering::Relaxed);

            report.samples += 1;
            report.cpu_total += cpu as u64;
            report.active_total += active as u64;
            report.queue_len_total += qlen as u64;
            if qlen > report.queue_len_max {
                report.queue_len_max = qlen;
            }

            let elapsed_ms = sim_start.elapsed().as_millis();
            // Lightweight log line; extremely spammy but helpful
            println!("[monitor {:>5}ms] active {}/{} | cpu {}% | queue {} | total {}",elapsed_ms, active, workers, cpu, qlen, total_tasks);

            thread::sleep(tick);
        }

        report
    })
}
