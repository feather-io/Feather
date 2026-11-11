#import "Basic";
#import "Thread";



// A synchronized, optionally-buffered message channel between fibers.
//
// This is Feather's implementation of Go-style channels. Fibers can send and
// receive values across fiber boundaries without shared-memory data races. The
// channel is protected by a `Mutex` and stores pending sender/receiver Futures
// so the Waker can wake them when space/data becomes available.
//
// **Buffering Modes**:
// - `capacity == 0` (unbuffered): The sender blocks until a receiver takes the value.
//   This guarantees a "rendezvous" - both sender and receiver are synchronized at
//   the moment of the handoff.
// - `capacity > 0` (buffered): The sender blocks only when the ring buffer is full.
//   Receivers block only when the ring buffer is empty.
//
// The internal ring buffer uses `head` and `tail` indices with modular arithmetic,
// avoiding repeated allocations.
Feather_Channel :: struct(T: Type) {
    // The internal ring buffer holding queued values.
    buffer: [] T;
    // Index of the oldest item in the buffer (next to be consumed).
    head: s64;
    // Index of the next write position in the ring buffer.
    tail: s64;
    // Current number of items in the buffer.
    count: s64;
    // The declared buffer capacity (0 means unbuffered).
    capacity: s64;

    // Futures of fibers blocked waiting to send (buffer full).
    pending_senders: [..] *Future;
    // Futures of fibers blocked waiting to receive (buffer empty).
    pending_receivers: [..] *Future;

    // Set to true by channel_close(). All pending waiters are woken on close.
    closed: bool;
    // Guards all fields above from concurrent access by multiple fibers.
    mutex: Mutex;
}

// Create and initialize a new channel of type T.
//
// For unbuffered channels (`capacity == 0`), an internal buffer of size 1 is
// allocated to hold the in-flight item during the sender/receiver rendezvous.
channel_make :: ($T: Type, capacity: int = 0) -> *Feather_Channel(T) {
    ch := New(Feather_Channel(T));
    ch.capacity = capacity;

    // For unbuffered, we allocate an internal buffer of 1 to hold the rendezvous item.
    internal_cap := ifx capacity == 0 then 1 else capacity;
    ch.buffer = NewArray(internal_cap, T);

    init(*ch.mutex);
    return ch;
}

// Close the channel and wake all pending fibers.
//
// Once closed, `channel_recv` returns `(zero_value, false)` once the buffer is drained.
// Any subsequent `channel_send` will assert-fail. All blocked senders and receivers are
// immediately woken so they can observe the closed state.
channel_close :: (ch: *Feather_Channel($T)) {
    lock(*ch.mutex);
    ch.closed = true;

    for w: ch.pending_receivers { wake(w); }
    ch.pending_receivers.count = 0;

    for w: ch.pending_senders { wake(w); }
    ch.pending_senders.count = 0;

    unlock(*ch.mutex);
}

// Release all memory associated with the channel.
channel_free :: (ch: *Feather_Channel($T)) {
    array_free(ch.buffer);
    array_free(ch.pending_senders);
    array_free(ch.pending_receivers);
    destroy(*ch.mutex);
    free(ch);
}

// Send a value into the channel, blocking the fiber if the buffer is full.
//
// For unbuffered channels, the sender additionally waits for a receiver to
// consume the item before returning - this is the rendezvous guarantee.
channel_send :: (ch: *Feather_Channel($T), value: T) {
    while true {
        lock(*ch.mutex);
        if ch.closed {
            unlock(*ch.mutex);
            assert(false, "send on closed channel");
            return;
        }

        limit := ifx ch.capacity == 0 then 1 else ch.capacity;

        if ch.count < limit {
            // Space available: write into the ring buffer.
            ch.buffer[ch.tail] = value;
            ch.tail = (ch.tail + 1) % limit;
            ch.count += 1;

            // Wake one receiver if any
            if ch.pending_receivers.count > 0 {
                w := ch.pending_receivers[0];
                array_ordered_remove_by_index(*ch.pending_receivers, 0);
                wake(w);
            }

            // For unbuffered channels: block until the item is consumed (rendezvous).
            if ch.capacity == 0 {
                fut := context.waker.fut;
                array_add(*ch.pending_senders, fut);
                unlock(*ch.mutex);

                // Wait for receiver to take it
                while true {
                    lock(*ch.mutex);
                    // It's consumed when count == 0
                    if ch.count == 0 {
                        unlock(*ch.mutex);
                        return; // Successfully handed off!
                    }
                    if ch.closed {
                        unlock(*ch.mutex);
                        assert(false, "channel closed while waiting for rendezvous");
                        return;
                    }
                    unlock(*ch.mutex);
                    fiber_yield();
                }
            } else {
                unlock(*ch.mutex);
                return;
            }
        } else {
            // Buffer is full: register this fiber as a pending sender and yield.
            fut := context.waker.fut;
            array_add(*ch.pending_senders, fut);
            unlock(*ch.mutex);
            fiber_yield();
        }
    }
}

// Receive a value from the channel, blocking the fiber if the buffer is empty.
//
// Returns `(value, true)` if a value was received, or `(zero_value, false)` if the
// channel is closed and drained.
channel_recv :: (ch: *Feather_Channel($T)) -> (T, bool) {
    while true {
        lock(*ch.mutex);

        limit := ifx ch.capacity == 0 then 1 else ch.capacity;

        if ch.count > 0 {
            // Data available: read from ring buffer.
            value := ch.buffer[ch.head];
            ch.head = (ch.head + 1) % limit;
            ch.count -= 1;

            // Wake one pending sender now that space has opened.
            if ch.pending_senders.count > 0 {
                w := ch.pending_senders[0];
                array_ordered_remove_by_index(*ch.pending_senders, 0);
                wake(w);
            }

            unlock(*ch.mutex);
            return value, true;
        } else if ch.closed {
            unlock(*ch.mutex);
            var: T; // Zero value
            return var, false;
        } else {
            // Buffer empty: register this fiber as a pending receiver and yield.
            fut := context.waker.fut;
            array_add(*ch.pending_receivers, fut);
            unlock(*ch.mutex);
            fiber_yield();
        }
    }
}

// Attempt to receive from the channel without blocking.
//
// Returns `(value, true)` if data was available, or `(zero_value, false)` immediately
// if the buffer is empty. Does not yield the fiber.
channel_try_recv :: (ch: *Feather_Channel($T)) -> (T, bool) {
    lock(*ch.mutex);
    limit := ifx ch.capacity == 0 then 1 else ch.capacity;

    if ch.count > 0 {
        value := ch.buffer[ch.head];
        ch.head = (ch.head + 1) % limit;
        ch.count -= 1;

        if ch.pending_senders.count > 0 {
            w := ch.pending_senders[0];
            array_ordered_remove_by_index(*ch.pending_senders, 0);
            wake(w);
        }

        unlock(*ch.mutex);
        return value, true;
    }

    unlock(*ch.mutex);
    var: T;
    return var, false;
}
