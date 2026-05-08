/*
This just frameworks the Task structs and the methods it needs to report
to the workers and manager. 
*/

use std::time::{Duration, Instant};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TaskKind {
    Cpu,
    Io,
}

impl TaskKind {
    /// From the amendments: CPU = 35%, IO = 10% of the global CPU budget.
    pub fn cpu_cost(self) -> u8 {
        match self {
            TaskKind::Cpu => 35,
            TaskKind::Io => 10,
        }
    }
}

#[derive(Clone, Debug)]
pub struct Task {
    pub id: u64,
    pub arrival_time: Instant,
    pub kind: TaskKind,
    pub duration: Duration,
    pub cpu_cost: u8,
}

impl Task {
    pub fn new(id: u64, kind: TaskKind, duration: Duration, arrival_time: Instant) -> Self {
        Self {
            id,
            arrival_time,
            kind,
            duration,
            cpu_cost: kind.cpu_cost(),
        }
    }
}
