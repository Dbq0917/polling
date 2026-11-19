//! A buffered event source.
//!
//! This module provides a small wrapper around a non-blocking stream that keeps
//! the read and write interests registered with a [`Poller`] in sync with the
//! state of its write buffer.

use std::collections::VecDeque;
use std::io::{self, Write};

use crate::{Event, Poller};

#[cfg(target_os = "hermit")]
use std::os::hermit::io::{AsFd, AsRawFd};
#[cfg(unix)]
use std::os::unix::io::{AsFd, AsRawFd};
#[cfg(windows)]
use std::os::windows::io::{AsRawSocket, AsSocket};

/// A stream with an internal buffer for pending writes.
///
/// Write interest is derived from the contents of the buffer:
///
/// - While there is pending data, [`Event::writable`] is included in the
///   interest so the source is woken up again after the socket drains.
/// - Once the buffer is empty, write interest is removed so an idle source does
///   not keep spinning in the event loop.
///
/// The source is not registered automatically. Use [`EventSource::register`]
/// first and [`EventSource::reregister`] after handling an event.
///
/// # Examples
///
/// ```no_run
/// use polling::{Event, EventSource, Poller};
/// use std::net::TcpStream;
///
/// # fn main() -> std::io::Result<()> {
/// let stream = TcpStream::connect("127.0.0.1:8000")?;
/// stream.set_nonblocking(true)?;
///
/// let poller = Poller::new()?;
/// let mut source = EventSource::new(stream, 1);
/// source.queue_write(b"hello");
///
/// unsafe { source.register(&poller)? };
///
/// let mut events = polling::Events::new();
/// poller.wait(&mut events, None)?;
///
/// for event in events.iter() {
///     if event.writable {
///         source.flush(64 * 1024)?;
///         source.reregister(&poller)?;
///     }
/// }
///
/// source.delete(&poller)?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
pub struct EventSource<S> {
    source: S,
    key: usize,
    readable: bool,
    write_buffer: VecDeque<u8>,
}

impl<S> EventSource<S> {
    /// Creates a new event source with an empty write buffer.
    ///
    /// The source should be in non-blocking mode before it is registered.
    pub fn new(source: S, key: usize) -> Self {
        Self {
            source,
            key,
            readable: false,
            write_buffer: VecDeque::new(),
        }
    }

    /// Returns the key used for events from this source.
    pub fn key(&self) -> usize {
        self.key
    }

    /// Returns the current interest in this source.
    ///
    /// Read interest is controlled by [`EventSource::set_readable`]. Write
    /// interest is enabled exactly while there is buffered data.
    pub fn interest(&self) -> Event {
        Event::new(self.key, self.readable, !self.write_buffer.is_empty())
    }

    /// Enables or disables interest in readable events.
    pub fn set_readable(&mut self, readable: bool) {
        self.readable = readable;
    }

    /// Returns the number of bytes waiting in the write buffer.
    pub fn pending_write_bytes(&self) -> usize {
        self.write_buffer.len()
    }

    /// Returns a reference to the wrapped source.
    pub fn get_ref(&self) -> &S {
        &self.source
    }

    /// Returns a mutable reference to the wrapped source.
    pub fn get_mut(&mut self) -> &mut S {
        &mut self.source
    }
}

impl<S: Write> EventSource<S> {
    /// Appends data to the write buffer.
    ///
    /// The data is not written to the source until [`EventSource::flush`] is
    /// called.
    pub fn queue_write(&mut self, data: &[u8]) {
        self.write_buffer.extend(data.iter().copied());
    }

    /// Writes buffered data to the source, processing at most `max_bytes`.
    ///
    /// Returns the number of bytes written. When the source returns
    /// [`io::ErrorKind::WouldBlock`], the remaining data stays in the buffer
    /// and the caller should keep write interest registered. Once the buffer is
    /// empty, [`EventSource::interest`] stops advertising writability.
    ///
    /// A non-zero `max_bytes` bound prevents one event from monopolizing the
    /// event loop when a large amount of data is queued.
    pub fn flush(&mut self, max_bytes: usize) -> io::Result<usize> {
        let mut written = 0;

        while written < max_bytes && !self.write_buffer.is_empty() {
            let limit = max_bytes - written;
            let (front, back) = self.write_buffer.as_slices();
            let slice = if front.is_empty() { back } else { front };
            let chunk = &slice[..slice.len().min(limit)];

            match self.source.write(chunk) {
                Ok(0) => {
                    return Err(io::Error::new(
                        io::ErrorKind::WriteZero,
                        "event source failed to write buffered data",
                    ));
                }
                Ok(n) => {
                    self.write_buffer.drain(..n);
                    written += n;
                }
                Err(err) if err.kind() == io::ErrorKind::WouldBlock => break,
                Err(err) if err.kind() == io::ErrorKind::Interrupted => break,
                Err(err) => return Err(err),
            }
        }

        Ok(written)
    }
}

#[cfg(any(unix, target_os = "hermit"))]
impl<S: AsFd + Write> EventSource<S> {
    /// Registers this source with the poller.
    ///
    /// # Safety
    ///
    /// The source must be deleted from the poller before this `EventSource` is
    /// dropped.
    pub unsafe fn register(&self, poller: &Poller) -> io::Result<()> {
        poller.add(self.source.as_fd().as_raw_fd(), self.interest())
    }

    /// Updates the registered interest to match the current write buffer.
    pub fn reregister(&self, poller: &Poller) -> io::Result<()> {
        poller.modify(self.source.as_fd(), self.interest())
    }

    /// Removes this source from the poller.
    pub fn delete(&self, poller: &Poller) -> io::Result<()> {
        poller.delete(self.source.as_fd())
    }
}

#[cfg(windows)]
impl<S: AsSocket + Write> EventSource<S> {
    /// Registers this source with the poller.
    ///
    /// # Safety
    ///
    /// The source must be deleted from the poller before this `EventSource` is
    /// dropped.
    pub unsafe fn register(&self, poller: &Poller) -> io::Result<()> {
        poller.add(self.source.as_socket().as_raw_socket(), self.interest())
    }

    /// Updates the registered interest to match the current write buffer.
    pub fn reregister(&self, poller: &Poller) -> io::Result<()> {
        poller.modify(self.source.as_socket(), self.interest())
    }

    /// Removes this source from the poller.
    pub fn delete(&self, poller: &Poller) -> io::Result<()> {
        poller.delete(self.source.as_socket())
    }
}
