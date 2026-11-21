use polling::{Event, Events, Poller};
use std::io::{self, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

#[test]
fn basic_io() {
    let poller = Poller::new().unwrap();
    let (read, mut write) = tcp_pair().unwrap();
    unsafe {
        poller.add(&read, Event::readable(1)).unwrap();
    }

    // Nothing should be available at first.
    let mut events = Events::new();
    assert_eq!(
        poller
            .wait(&mut events, Some(Duration::from_secs(0)))
            .unwrap(),
        0
    );
    assert!(events.is_empty());

    // After a write, the event should be available now.
    write.write_all(&[1]).unwrap();
    assert_eq!(
        poller
            .wait(&mut events, Some(Duration::from_secs(1)))
            .unwrap(),
        1
    );

    assert_eq!(events.len(), 1);
    assert_eq!(
        events.iter().next().unwrap().with_no_extra(),
        Event::readable(1)
    );
    poller.delete(&read).unwrap();
}

#[test]
fn insert_twice() {
    #[cfg(unix)]
    use std::os::unix::io::AsRawFd;
    #[cfg(windows)]
    use std::os::windows::io::AsRawSocket;

    let (read, mut write) = tcp_pair().unwrap();
    let read = Arc::new(read);

    let poller = Poller::new().unwrap();
    unsafe {
        #[cfg(unix)]
        let read = read.as_raw_fd();
        #[cfg(windows)]
        let read = read.as_raw_socket();

        // Re-registering the same source merges the new interest into the existing
        // entry instead of replacing it.
        poller.add(read, Event::readable(1)).unwrap();
        poller.add(read, Event::writable(2)).unwrap();
    }

    // The socket is already writable, so the write interest fires first.
    let mut events = Events::new();
    poller
        .wait(&mut events, Some(Duration::from_secs(1)))
        .unwrap();
    assert!(events
        .iter()
        .any(|event| event.with_no_extra() == Event::writable(2)));

    // Re-arm the writable slot while keeping the readable slot registered.
    events.clear();
    unsafe {
        #[cfg(unix)]
        let raw = read.as_raw_fd();
        #[cfg(windows)]
        let raw = read.as_raw_socket();
        poller.add(raw, Event::writable(2)).unwrap();
    }

    // The readable slot must still be alive after the second registration.
    write.write_all(&[1]).unwrap();
    events.clear();
    poller
        .wait(&mut events, Some(Duration::from_secs(1)))
        .unwrap();
    assert!(events
        .iter()
        .any(|event| event.with_no_extra() == Event::readable(1)));

    poller.delete(&read).unwrap();
}

/// Two parties register read and write interest in the same source from two
/// goroutines. Both interests must survive and both parties must receive their
/// own event, even though the source is only registered once in the kernel.
#[test]
fn shared_read_write_interest() {
    #[cfg(unix)]
    use std::os::unix::io::AsRawFd;
    #[cfg(windows)]
    use std::os::windows::io::AsRawSocket;

    const READ_KEY: usize = 1;
    const WRITE_KEY: usize = 2;

    let (read, mut write) = tcp_pair().unwrap();
    let read = Arc::new(read);
    let poller = Arc::new(Poller::new().unwrap());

    let read_for_writer = Arc::clone(&read);
    let poller_for_writer = Arc::clone(&poller);
    let writer_thread = thread::spawn(move || {
        #[cfg(unix)]
        let raw = read_for_writer.as_raw_fd();
        #[cfg(windows)]
        let raw = read_for_writer.as_raw_socket();
        unsafe {
            poller_for_writer
                .add(raw, Event::writable(WRITE_KEY))
                .unwrap();
        }
    });

    #[cfg(unix)]
    let raw = read.as_raw_fd();
    #[cfg(windows)]
    let raw = read.as_raw_socket();
    unsafe {
        poller.add(raw, Event::readable(READ_KEY)).unwrap();
    }
    writer_thread.join().unwrap();

    let mut events = Events::new();
    let mut got_read = false;
    let mut got_write = false;

    // Drive the single-waiter event loop until both parties are notified.
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while !(got_read && got_write) {
        events.clear();
        let remaining = deadline.saturating_duration_since(std::time::Instant::now());
        poller
            .wait(&mut events, Some(remaining))
            .expect("wait failed before both interests fired");
        assert!(
            std::time::Instant::now() < deadline,
            "timed out waiting for shared interests: got_read={got_read}, got_write={got_write}"
        );

        for event in events.iter() {
            match event.with_no_extra() {
                Event {
                    key: READ_KEY,
                    readable: true,
                    writable: false,
                    ..
                } => {
                    got_read = true;
                }
                Event {
                    key: WRITE_KEY,
                    readable: false,
                    writable: true,
                    ..
                } => {
                    got_write = true;
                }
                other => panic!("unexpected event: {other:?}"),
            }
        }

        // Oneshot slots need to be re-armed after each delivery.
        if got_write {
            // The write slot may have disarmed; re-register it. This must not
            // revoke the readable slot of the other party.
            unsafe {
                poller.add(raw, Event::writable(WRITE_KEY)).unwrap();
            }
        }
        if !got_read {
            write.write_all(&[1]).unwrap();
        }
    }

    assert!(got_read && got_write);

    // Revoking the write party must not remove the readable party's slot.
    poller.modify(&*read, Event::readable(READ_KEY)).unwrap();
    write.write_all(&[2]).unwrap();
    events.clear();
    poller
        .wait(&mut events, Some(Duration::from_secs(1)))
        .unwrap();
    assert_eq!(
        events.iter().next().unwrap().with_no_extra(),
        Event::readable(READ_KEY)
    );

    poller.delete(&read).unwrap();
    assert_eq!(
        poller.delete(&read).unwrap_err().kind(),
        io::ErrorKind::NotFound
    );
}

#[test]
fn delete_unrecognized() {
    let (read, _) = tcp_pair().unwrap();
    let read = Arc::new(read);

    let poller = Poller::new().unwrap();
    match poller.delete(&read) {
        Ok(_) => panic!("delete of unregistered source succeeded"),
        Err(e) => assert_eq!(
            e.kind(),
            io::ErrorKind::NotFound,
            "wrong error kind for delete of unregistered source",
        ),
    }
}

/// Test that calling `wait` appends events, as [documented], rather than
/// overwriting them.
///
/// [documented]: https://docs.rs/polling/latest/polling/struct.Poller.html#method.wait
#[test]
fn append_events() {
    #[cfg(unix)]
    use std::os::unix::io::AsRawFd;
    #[cfg(windows)]
    use std::os::windows::io::AsRawSocket;

    // Create a few sockets.
    let mut pairs = Vec::new();
    for _ in 0..4 {
        let (read, write) = tcp_pair().unwrap();
        pairs.push((read, write));
    }

    // Add the sockets to the poller.
    let poller = Poller::new().unwrap();
    unsafe {
        for (read, _write) in &pairs {
            #[cfg(unix)]
            let read = read.as_raw_fd();
            #[cfg(windows)]
            let read = read.as_raw_socket();

            poller.add(read, Event::readable(1)).unwrap();
        }
    }

    // Trigger read events on the sockets and reuse the event list to test
    // that events are appended.
    let mut events = Events::new();

    for (index, (_read, ref mut write)) in pairs.iter_mut().enumerate() {
        // Write to the socket prompting a reader readiness event.
        write.write_all(&[1]).unwrap();
        assert_eq!(
            poller
                .wait(&mut events, Some(Duration::from_secs(1)))
                .unwrap(),
            index + 1
        );
        assert_eq!(events.len(), index + 1);
        for event in events.iter() {
            assert_eq!(event.with_no_extra(), Event::readable(1));
        }
    }

    for (read, _write) in &pairs {
        poller.delete(read).unwrap();
    }
}

fn tcp_pair() -> io::Result<(TcpStream, TcpStream)> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let a = TcpStream::connect(listener.local_addr()?)?;
    let (b, _) = listener.accept()?;
    Ok((a, b))
}
