// This is where our simulation presets live
// They implement both FIFO and an optimized "packed" policy

#[derive(Clone, Copy, Debug)]
pub enum Policy {
    /// The FIFO approach, not fast.
    Fifo,
    /// Pack CPU + IO so that we max out to 100% CPU utilization.
    Optimize,
}

#[derive(Clone, Copy, Debug)]
pub enum WorkloadKind {
    /// 70% IO / 30% CPU — the "balanced" sim preset.
    Balanced,
    /// 80% IO / 20% CPU — mix and match your tasks like in the lecture.
    /// A CPU-heavy preset to stress FIFO harder.
    Stressed,
}

#[derive(Clone, Debug)]
pub struct Params {
    pub workers: usize,
    pub total_tasks: u64,
    pub arrival_interval_ms: u64,
    pub task_duration_ms: u64,
    pub io_probability: f64,
    pub cpu_budget: u8,
    pub monitor_tick_ms: u64,
    pub rng_seed: u64,
    pub policy: Policy,
    pub workload: WorkloadKind,
}

impl Params {
    pub fn balanced(policy: Policy) -> Self {
        Self {
            workers: 8,
            total_tasks: 1000,
            arrival_interval_ms: 20,
            task_duration_ms: 200,
            io_probability: 0.70,
            cpu_budget: 100,
            monitor_tick_ms: 10,
            rng_seed: 42,
            policy,
            workload: WorkloadKind::Balanced,
        }
    }

    pub fn stressed(policy: Policy) -> Self {
        Self {
            io_probability: 0.80,
            workload: WorkloadKind::Stressed,
            ..Self::balanced(policy)
        }
    }
}
