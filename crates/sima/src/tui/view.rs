//! The ratatui rendering of a [`ViewModel`]: header, worker panel, counters,
//! event log, and the key bar. Layout only — every value shown is already
//! resolved on the model, so this file holds no run logic.

use ratatui::Frame;
use ratatui::layout::{Constraint, Direction, Layout};
use ratatui::widgets::{Block, Borders, List, ListItem, Paragraph};

use super::state::ViewModel;
use crate::render::short;

/// Draws the whole screen for `vm` into `frame`.
pub fn draw(frame: &mut Frame, vm: &ViewModel) {
    // One row per worker, plus the block's two border rows.
    let workers_height = vm.workers.len() as u16 + 2;
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),
            Constraint::Length(workers_height),
            Constraint::Length(3),
            Constraint::Min(3),
            Constraint::Length(1),
        ])
        .split(frame.area());

    let header = Paragraph::new(format!("run {}    state {}", short(&vm.run), vm.state))
        .block(Block::default().borders(Borders::ALL).title("sima tui"));
    frame.render_widget(header, chunks[0]);

    let workers: Vec<ListItem> = vm
        .workers
        .iter()
        .map(|row| {
            let line = match &row.lease {
                Some(lease) => format!(
                    "worker {}   {} (attempt {})",
                    row.worker,
                    short(&lease.task),
                    lease.attempt
                ),
                None => format!("worker {}   idle", row.worker),
            };
            ListItem::new(line)
        })
        .collect();
    frame.render_widget(
        List::new(workers).block(Block::default().borders(Borders::ALL).title("workers")),
        chunks[1],
    );

    let counters = Paragraph::new(format!(
        "committed {}/{}    retried {}    rejected {}    faulted {}    lease expired {}",
        vm.committed, vm.tasks, vm.retried, vm.rejected, vm.faulted, vm.lease_expired
    ))
    .block(Block::default().borders(Borders::ALL).title("counters"));
    frame.render_widget(counters, chunks[2]);

    // The log holds lines oldest-first; show the tail that fits the box so
    // the most recent events stay visible.
    let visible = chunks[3].height.saturating_sub(2) as usize;
    let start = vm.log.len().saturating_sub(visible);
    let log: Vec<ListItem> = vm.log[start..]
        .iter()
        .map(|line| ListItem::new(line.clone()))
        .collect();
    frame.render_widget(
        List::new(log).block(Block::default().borders(Borders::ALL).title("events")),
        chunks[3],
    );

    frame.render_widget(
        Paragraph::new("s start   x stop   q quit   Q force quit"),
        chunks[4],
    );
}
