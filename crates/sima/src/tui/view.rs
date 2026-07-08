//! The ratatui rendering of a [`ViewModel`]: header, worker panel, counters,
//! and event log, with a help overlay drawn over them on request. Layout only
//! — every value shown is already resolved on the model, so this file holds no
//! run logic.

use ratatui::Frame;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::widgets::{Block, Borders, Clear, List, ListItem, Paragraph};

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

    if vm.help {
        draw_help(frame);
    }
}

/// The key bindings the help overlay lists, one per line.
const HELP_LINES: &str = "\
s   start\n\
x   stop (also Ctrl-C)\n\
q   quit\n\
Q   force quit\n\
?   help";

/// Draws the help overlay: a bordered block listing every key binding,
/// centered over the frame on a cleared background so the screen beneath does
/// not show through.
fn draw_help(frame: &mut Frame) {
    // Five binding lines plus the block's two border rows; wide enough for the
    // longest line, bounded by the frame so a small terminal still fits it.
    let area = centered(frame.area(), 24, 7);
    let overlay =
        Paragraph::new(HELP_LINES).block(Block::default().borders(Borders::ALL).title("help"));
    frame.render_widget(Clear, area);
    frame.render_widget(overlay, area);
}

/// A `width`×`height` rectangle centered within `area`, clamped to it so the
/// overlay never overflows a small frame.
fn centered(area: Rect, width: u16, height: u16) -> Rect {
    let width = width.min(area.width);
    let height = height.min(area.height);
    Rect {
        x: area.x + (area.width - width) / 2,
        y: area.y + (area.height - height) / 2,
        width,
        height,
    }
}
