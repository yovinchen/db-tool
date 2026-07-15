use crossterm::{
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use std::io;

pub(crate) trait TerminalLifecycle {
    fn enable_raw_mode(&mut self) -> io::Result<()>;
    fn enter_alternate_screen(&mut self) -> io::Result<()>;
    fn leave_alternate_screen(&mut self) -> io::Result<()>;
    fn disable_raw_mode(&mut self) -> io::Result<()>;
}

pub(crate) struct CrosstermLifecycle;

impl TerminalLifecycle for CrosstermLifecycle {
    fn enable_raw_mode(&mut self) -> io::Result<()> {
        enable_raw_mode()
    }

    fn enter_alternate_screen(&mut self) -> io::Result<()> {
        execute!(io::stdout(), EnterAlternateScreen)
    }

    fn leave_alternate_screen(&mut self) -> io::Result<()> {
        execute!(io::stdout(), LeaveAlternateScreen)
    }

    fn disable_raw_mode(&mut self) -> io::Result<()> {
        disable_raw_mode()
    }
}

/// Owns the process-wide terminal modes for one TUI session.
///
/// The flags are set only after the corresponding enter operation succeeds, so
/// a partially initialized session restores exactly the state it changed. Drop
/// is the unwind/early-return backstop; normal execution calls `restore`
/// explicitly so cleanup failures remain observable.
pub(crate) struct TerminalSession<L: TerminalLifecycle> {
    lifecycle: L,
    raw_mode: bool,
    alternate_screen: bool,
}

impl<L: TerminalLifecycle> TerminalSession<L> {
    pub(crate) fn enter(lifecycle: L) -> io::Result<Self> {
        let mut session = Self {
            lifecycle,
            raw_mode: false,
            alternate_screen: false,
        };

        session.lifecycle.enable_raw_mode()?;
        session.raw_mode = true;
        session.lifecycle.enter_alternate_screen()?;
        session.alternate_screen = true;
        Ok(session)
    }

    pub(crate) fn restore(&mut self) -> io::Result<()> {
        let mut first_error = None;

        if self.alternate_screen {
            let result = self.lifecycle.leave_alternate_screen();
            self.alternate_screen = false;
            if let Err(error) = result {
                first_error = Some(error);
            }
        }

        if self.raw_mode {
            let result = self.lifecycle.disable_raw_mode();
            self.raw_mode = false;
            if first_error.is_none() {
                if let Err(error) = result {
                    first_error = Some(error);
                }
            }
        }

        match first_error {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }
}

impl<L: TerminalLifecycle> Drop for TerminalSession<L> {
    fn drop(&mut self) {
        let _ = self.restore();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        panic::{catch_unwind, AssertUnwindSafe},
        sync::{Arc, Mutex},
    };

    #[derive(Clone, Copy)]
    enum Failure {
        None,
        EnterAlternate,
        LeaveAlternate,
    }

    struct RecordingLifecycle {
        events: Arc<Mutex<Vec<&'static str>>>,
        failure: Failure,
    }

    impl RecordingLifecycle {
        fn new(events: Arc<Mutex<Vec<&'static str>>>, failure: Failure) -> Self {
            Self { events, failure }
        }

        fn record(&self, event: &'static str) {
            self.events.lock().unwrap().push(event);
        }
    }

    impl TerminalLifecycle for RecordingLifecycle {
        fn enable_raw_mode(&mut self) -> io::Result<()> {
            self.record("enable_raw");
            Ok(())
        }

        fn enter_alternate_screen(&mut self) -> io::Result<()> {
            self.record("enter_alternate");
            if matches!(self.failure, Failure::EnterAlternate) {
                Err(io::Error::other("enter failed"))
            } else {
                Ok(())
            }
        }

        fn leave_alternate_screen(&mut self) -> io::Result<()> {
            self.record("leave_alternate");
            if matches!(self.failure, Failure::LeaveAlternate) {
                Err(io::Error::other("leave failed"))
            } else {
                Ok(())
            }
        }

        fn disable_raw_mode(&mut self) -> io::Result<()> {
            self.record("disable_raw");
            Ok(())
        }
    }

    #[test]
    fn partial_enter_failure_restores_raw_mode() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let result = TerminalSession::enter(RecordingLifecycle::new(
            Arc::clone(&events),
            Failure::EnterAlternate,
        ));

        assert!(result.is_err());
        assert_eq!(
            *events.lock().unwrap(),
            ["enable_raw", "enter_alternate", "disable_raw"]
        );
    }

    #[test]
    fn drop_restores_terminal_during_unwind() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let unwind = catch_unwind(AssertUnwindSafe({
            let events = Arc::clone(&events);
            move || {
                let _session =
                    TerminalSession::enter(RecordingLifecycle::new(events, Failure::None)).unwrap();
                panic!("simulated event-loop panic");
            }
        }));

        assert!(unwind.is_err());
        assert_eq!(
            *events.lock().unwrap(),
            [
                "enable_raw",
                "enter_alternate",
                "leave_alternate",
                "disable_raw"
            ]
        );
    }

    #[test]
    fn cleanup_attempts_raw_restore_when_leaving_screen_fails() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let mut session = TerminalSession::enter(RecordingLifecycle::new(
            Arc::clone(&events),
            Failure::LeaveAlternate,
        ))
        .unwrap();

        assert!(session.restore().is_err());
        assert_eq!(
            *events.lock().unwrap(),
            [
                "enable_raw",
                "enter_alternate",
                "leave_alternate",
                "disable_raw"
            ]
        );
    }
}
