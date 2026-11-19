use polling::{Event, EventSource, Events, Poller};
use std::io::{self, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;
use std::time::{Duration, Instant};

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

        poller.add(read, Event::readable(1)).unwrap();
        assert_eq!(
            poller.add(read, Event::readable(1)).unwrap_err().kind(),
            io::ErrorKind::AlreadyExists
        );
    }

    write.write_all(&[1]).unwrap();
    let mut events = Events::new();
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

#[test]
fn partial_write_resumes() {
    const PAYLOAD_SIZE: usize = 8 * 1024 * 1024 + 4096;
    const MAX_BYTES_PER_EVENT: usize = 64 * 1024;

    let (writer, mut reader) = tcp_pair().unwrap();
    writer.set_nonblocking(true).unwrap();
    reader.set_nonblocking(true).unwrap();

    let poller = Poller::new().unwrap();
    let mut source = EventSource::new(writer, 7);
    source.set_readable(true);

    // A single write larger than the socket buffer only completes partially.
    // Keep the remainder in the event source's internal buffer.
    let payload = vec![0xAB; PAYLOAD_SIZE];
    let first_write = source.get_mut().write(&payload).unwrap();
    assert!(first_write > 0);
    assert!(first_write < PAYLOAD_SIZE);
    source.queue_write(&payload[first_write..]);
    assert_eq!(source.interest(), Event::all(7));

    unsafe {
        source.register(&poller).unwrap();
    }

    let mut events = Events::new();
    let mut received = Vec::with_capacity(PAYLOAD_SIZE);
    let mut flushes = 0;
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut buf = [0; 64 * 1024];

    loop {
        match reader.read(&mut buf) {
            Ok(0) => break,
            Ok(n) => received.extend_from_slice(&buf[..n]),
            Err(err) if err.kind() == io::ErrorKind::WouldBlock => break,
            Err(err) => panic!("read failed: {err}"),
        }
    }

    while source.pending_write_bytes() > 0 {
        events.clear();
        assert!(
            poller
                .wait(&mut events, Some(Duration::from_secs(1)))
                .unwrap()
                > 0,
            "the source stopped receiving writable events with {} bytes pending",
            source.pending_write_bytes()
        );
        assert!(events.iter().any(|event| event.writable));
        assert!(Instant::now() < deadline, "partial write did not resume");

        let written = source.flush(MAX_BYTES_PER_EVENT).unwrap();
        assert!(written <= MAX_BYTES_PER_EVENT);

        // Keep write interest registered while data is pending, and drop it as
        // soon as the buffer is empty.
        source.reregister(&poller).unwrap();

        if written == 0 {
            continue;
        }
        flushes += 1;

        loop {
            match reader.read(&mut buf) {
                Ok(0) => break,
                Ok(n) => received.extend_from_slice(&buf[..n]),
                Err(err) if err.kind() == io::ErrorKind::WouldBlock => break,
                Err(err) => panic!("read failed: {err}"),
            }
        }
    }

    assert!(flushes > 1, "the payload was written in a single event");
    assert_eq!(source.interest(), Event::readable(7));

    let drain_deadline = Instant::now() + Duration::from_secs(30);
    while received.len() < PAYLOAD_SIZE {
        assert!(
            Instant::now() < drain_deadline,
            "received {} of {} bytes",
            received.len(),
            PAYLOAD_SIZE
        );
        match reader.read(&mut buf) {
            Ok(0) => break,
            Ok(n) => received.extend_from_slice(&buf[..n]),
            Err(err) if err.kind() == io::ErrorKind::WouldBlock => {
                std::thread::sleep(Duration::from_millis(1));
            }
            Err(err) => panic!("read failed: {err}"),
        }
    }
    assert_eq!(received.len(), PAYLOAD_SIZE);
    assert!(received.iter().all(|byte| *byte == 0xAB));

    // With an empty buffer there is no write interest, so an idle wait must not
    // keep returning writable events.
    events.clear();
    assert_eq!(
        poller
            .wait(&mut events, Some(Duration::from_millis(0)))
            .unwrap(),
        0
    );

    source.delete(&poller).unwrap();
}

fn tcp_pair() -> io::Result<(TcpStream, TcpStream)> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let a = TcpStream::connect(listener.local_addr()?)?;
    let (b, _) = listener.accept()?;
    Ok((a, b))
}
