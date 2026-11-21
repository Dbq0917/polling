//! Per-source interest registry that lets multiple parties share one registration.
//!
//! The platform pollers only keep one interest record per source (file descriptor or
//! socket). This layer keeps one [`Entry`] per source holding a separate slot for the
//! readable and writable direction. Several parties can therefore register interest in
//! the same source: their interest bits are merged into the single kernel registration,
//! and revoking one direction never touches the other party's slot.
//!
//! The kernel is still told about the source exactly once, so waiting and event
//! dispatch cost does not grow with the number of registered parties.

use std::collections::HashMap;
use std::io;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex;
use std::time::Instant;

use crate::sys;
use crate::{Event, PollMode, NOTIFY_KEY};

cfg_if::cfg_if! {
    if #[cfg(windows)] {
        use std::os::windows::io::{AsRawSocket, BorrowedSocket, RawSocket};

        /// Platform identifier of a registered source.
        type SourceId = RawSocket;

        /// Borrow the source back from its raw identifier.
        ///
        /// # Safety
        ///
        /// The source must outlive the returned borrow.
        unsafe fn borrow_source(id: SourceId) -> BorrowedSocket<'static> {
            unsafe { BorrowedSocket::borrow_raw(id) }
        }
    } else if #[cfg(target_os = "hermit")] {
        use std::os::hermit::io::{AsRawFd, BorrowedFd, RawFd};

        /// Platform identifier of a registered source.
        type SourceId = RawFd;

        /// Borrow the source back from its raw identifier.
        ///
        /// # Safety
        ///
        /// The source must outlive the returned borrow.
        unsafe fn borrow_source(id: SourceId) -> BorrowedFd<'static> {
            unsafe { BorrowedFd::borrow_raw(id) }
        }
    } else {
        use std::os::unix::io::{AsRawFd, BorrowedFd, RawFd};

        /// Platform identifier of a registered source.
        type SourceId = RawFd;

        /// Borrow the source back from its raw identifier.
        ///
        /// # Safety
        ///
        /// The source must outlive the returned borrow.
        unsafe fn borrow_source(id: SourceId) -> BorrowedFd<'static> {
            unsafe { BorrowedFd::borrow_raw(id) }
        }
    }
}

/// Interest registered by one party in one direction.
#[derive(Clone, Copy, Debug)]
struct Slot {
    /// Key reported back to this party when the direction becomes ready.
    key: usize,
    /// Polling mode the direction was registered with.
    mode: PollMode,
}

/// All interest registered in a single source.
#[derive(Clone, Copy, Debug, Default)]
struct Entry {
    /// The party interested in readability, if any.
    read: Option<Slot>,
    /// The party interested in writability, if any.
    write: Option<Slot>,
    /// Whether the source was registered in the kernel and not explicitly deleted.
    ///
    /// Oneshot backends can drop the in-kernel registration on their own once an
    /// event fires; [`sync_entry`] heals that mismatch by retrying a failed modify
    /// as an add, so this flag tracks our own intent rather than kernel state.
    armed: bool,
}

impl Entry {
    /// Interest handed to the kernel.
    ///
    /// The reported key is replaced by the source identifier so that incoming events
    /// can be routed back to this entry; per-party keys are restored at dispatch time.
    fn kernel_event(&self, source: SourceId) -> Event {
        let mut event = Event::new(0, self.read.is_some(), self.write.is_some());
        event.key = source as usize;
        event
    }

    /// Polling mode to program the kernel registration with.
    fn kernel_mode(&self) -> PollMode {
        self.read
            .or(self.write)
            .map(|slot| slot.mode)
            .unwrap_or(PollMode::Oneshot)
    }
}

/// Returns `true` if the mode disarms the source after one delivered event.
fn is_oneshot(mode: PollMode) -> bool {
    matches!(mode, PollMode::Oneshot | PollMode::EdgeOneshot)
}

/// Pushes the current state of `entry` for `source` into the kernel poller.
///
/// A source with no interested party is registered with zero interest bits so
/// that a later `modify` can arm it in place; it is only removed from the kernel
/// when the entry is deleted. The source is added once and modified afterwards.
///
/// Some backends (notably epoll in oneshot mode) drop the kernel registration as
/// soon as an event is delivered. In that case the in-kernel state can lag behind
/// `armed`, so a failed modify is retried as an add.
fn sync_entry(poller: &sys::Poller, source: SourceId, entry: &mut Entry) -> io::Result<()> {
    let kernel_event = entry.kernel_event(source);
    let mode = entry.kernel_mode();
    if entry.armed {
        // SAFETY: The source is registered and remains valid.
        let borrowed = unsafe { borrow_source(source) };
        match poller.modify(borrowed, kernel_event, mode) {
            Err(err) if err.kind() == io::ErrorKind::NotFound => {
                // The backend dropped the registration (e.g. a fired oneshot).
                unsafe { poller.add(source, kernel_event, mode) }?;
                entry.armed = true;
            }
            result => result?,
        }
    } else {
        unsafe { poller.add(source, kernel_event, mode) }?;
        entry.armed = true;
    }
    Ok(())
}

/// Shares one kernel registration per source between multiple parties.
pub(crate) struct Registry {
    /// The platform-specific poller holding the actual kernel registrations.
    ///
    /// Kept outside the entries lock so `notify` and the kernel wait do not block
    /// concurrent registration changes.
    poller: sys::Poller,
    /// One entry per registered source.
    entries: Mutex<HashMap<SourceId, Entry>>,
    /// Reusable buffer for raw events returned by the platform poller.
    sys_events: Mutex<sys::Events>,
    /// Set by an explicit [`Registry::notify`] to request that a running wait return
    /// as soon as it reaches a quiescent point, even with no user events.
    ///
    /// Notifications generated internally by registration changes do not set this, so
    /// a wait interrupted by its own `add`/`modify`/`delete` keeps waiting for the real
    /// event instead of returning an empty event set.
    stop_requested: AtomicBool,
}

impl Registry {
    /// Creates a new registry backed by a fresh platform poller.
    pub(crate) fn new() -> io::Result<Self> {
        Ok(Self {
            poller: sys::Poller::new()?,
            entries: Mutex::new(HashMap::new()),
            sys_events: Mutex::new(sys::Events::with_capacity(1024)),
            stop_requested: AtomicBool::new(false),
        })
    }

    /// Whether the platform poller supports level-triggered polling.
    pub(crate) fn supports_level(&self) -> bool {
        self.poller.supports_level()
    }

    /// Whether the platform poller supports edge-triggered polling.
    pub(crate) fn supports_edge(&self) -> bool {
        self.poller.supports_edge()
    }

    /// Adds interest in a source.
    ///
    /// If the source is already registered, the new interest is merged into the
    /// existing entry instead of replacing it.
    ///
    /// # Safety
    ///
    /// The source must be valid and outlive its registration.
    pub(crate) unsafe fn add(
        &self,
        source: impl crate::AsRawSource,
        event: Event,
        mode: PollMode,
    ) -> io::Result<()> {
        let id = source.raw();
        let mut entries = self.entries.lock().unwrap();

        let entry = entries.entry(id).or_default();
        if event.readable {
            entry.read = Some(Slot {
                key: event.key,
                mode,
            });
        }
        if event.writable {
            entry.write = Some(Slot {
                key: event.key,
                mode,
            });
        }

        sync_entry(&self.poller, id, entry)?;
        drop(entries);
        self.commit_change()
    }

    /// Replaces the interest in an already registered source.
    ///
    /// Directions set to `false` are revoked; directions set to `true` keep or
    /// establish a slot. Other slots of the entry are left alone unless explicitly
    /// revoked here.
    pub(crate) fn modify(
        &self,
        source: impl crate::AsSource,
        event: Event,
        mode: PollMode,
    ) -> io::Result<()> {
        cfg_if::cfg_if! {
            if #[cfg(windows)] {
                let id = source.source().as_raw_socket();
            } else {
                let id = source.source().as_raw_fd();
            }
        }

        let mut entries = self.entries.lock().unwrap();
        let entry = entries
            .get_mut(&id)
            .ok_or(io::Error::from(io::ErrorKind::NotFound))?;

        if event.readable {
            entry.read = Some(Slot {
                key: event.key,
                mode,
            });
        } else {
            entry.read = None;
        }

        if event.writable {
            entry.write = Some(Slot {
                key: event.key,
                mode,
            });
        } else {
            entry.write = None;
        }

        // Empty interest disarms every direction in the kernel but keeps the source
        // registered, so a later `add` or `modify` re-arms it and `delete` stays the
        // only way to report `NotFound` for an unknown source.
        sync_entry(&self.poller, id, entry)?;
        drop(entries);
        self.commit_change()
    }

    /// Removes a source and every interest registered for it.
    pub(crate) fn delete(&self, source: impl crate::AsSource) -> io::Result<()> {
        cfg_if::cfg_if! {
            if #[cfg(windows)] {
                let borrowed = source.source();
                let id = borrowed.as_raw_socket();
            } else {
                let borrowed = source.source();
                let id = borrowed.as_raw_fd();
            }
        }

        let mut entries = self.entries.lock().unwrap();
        match entries.remove(&id) {
            Some(entry) => {
                let result = if entry.armed {
                    self.poller.delete(borrowed)
                } else {
                    Ok(())
                };
                result?;
                drop(entries);
                self.commit_change()
            }
            None => Err(io::Error::from(io::ErrorKind::NotFound)),
        }
    }

    /// Records a registration change and wakes a thread blocked in the kernel wait.
    ///
    /// Registry mutations take the same lock as the wait path, so without an explicit
    /// wake-up a concurrent `wait` could sleep until its timeout even though the new
    /// interest is already able to produce an event.
    fn commit_change(&self) -> io::Result<()> {
        self.poller.notify()
    }

    /// Waits for events until the deadline and appends demultiplexed events to `out`.
    pub(crate) fn wait_deadline(
        &self,
        out: &mut Vec<Event>,
        deadline: Option<Instant>,
    ) -> io::Result<()> {
        let mut sys_events = self.sys_events.lock().unwrap();
        let out_len = out.len();

        loop {
            sys_events.clear();

            // The kernel wait does not take the entries lock, so a concurrent
            // registration change can run and notify us to break out early.
            self.poller.wait_deadline(&mut sys_events, deadline)?;

            // Snapshot the raw events; each one carries the source identifier as its key.
            let raw: Vec<Event> = sys_events.iter().collect();
            let mut entries = self.entries.lock().unwrap();

            // Sources whose oneshot slot fired and whose kernel registration must be
            // refreshed with the remaining interest.
            let mut rearm: Vec<SourceId> = Vec::new();

            for event in raw {
                if event.key == NOTIFY_KEY {
                    continue;
                }
                let id = event.key as SourceId;
                let Some(entry) = entries.get_mut(&id) else {
                    // The source was deleted while the event was in flight.
                    continue;
                };

                if event.readable {
                    if let Some(slot) = entry.read {
                        out.push(Event {
                            key: slot.key,
                            readable: true,
                            writable: false,
                            extra: event.extra,
                        });
                        if is_oneshot(slot.mode) {
                            entry.read = None;
                            rearm.push(id);
                        }
                    }
                }

                if event.writable {
                    if let Some(slot) = entry.write {
                        out.push(Event {
                            key: slot.key,
                            readable: false,
                            writable: true,
                            extra: event.extra,
                        });
                        if is_oneshot(slot.mode) {
                            entry.write = None;
                            rearm.push(id);
                        }
                    }
                }
            }

            rearm.sort_unstable();
            rearm.dedup();
            for id in rearm {
                let Some(entry) = entries.get_mut(&id) else {
                    continue;
                };
                // Re-arm with the remaining interest, or leave the source unregistered
                // if every slot was consumed. Empty entries are retained so a later
                // `add` re-creates the kernel registration and `delete` stays the only
                // way to forget an unknown source (returning `NotFound` otherwise).
                sync_entry(&self.poller, id, entry)?;
            }

            let deadline_pending = deadline.map_or(true, |at| Instant::now() < at);
            // Consume any pending stop request every iteration so it cannot leak into
            // a later wait when this one returns for a user event or the deadline.
            let stop_requested = self.stop_requested.swap(false, Ordering::SeqCst);

            // The wait ends on a user event, an elapsed deadline, or an explicit
            // `notify`. Bare wake-ups from internal notifications (a concurrent
            // registration change or the platform poller's own notifier bookkeeping)
            // restart the wait with the remaining deadline instead of returning empty.
            if out.len() > out_len || !deadline_pending || stop_requested {
                return Ok(());
            }
        }
    }

    /// Wakes up the current or next wait.
    pub(crate) fn notify(&self) -> io::Result<()> {
        self.stop_requested.store(true, Ordering::SeqCst);
        self.poller.notify()
    }

    /// Returns the raw file descriptor of the platform poller.
    #[cfg(all(
        any(
            target_os = "linux",
            target_os = "android",
            target_os = "redox",
            target_os = "illumos",
            target_os = "solaris",
            target_vendor = "apple",
            target_os = "freebsd",
            target_os = "netbsd",
            target_os = "openbsd",
            target_os = "dragonfly",
        ),
        not(polling_test_poll_backend),
    ))]
    pub(crate) fn as_raw_fd(&self) -> RawFd {
        self.poller.as_raw_fd()
    }

    /// Returns the raw handle of the platform poller.
    #[cfg(windows)]
    pub(crate) fn as_raw_handle(&self) -> std::os::windows::io::RawHandle {
        self.poller.as_raw_handle()
    }
}
