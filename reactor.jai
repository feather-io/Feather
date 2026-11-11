#import "Basic";
#import "POSIX";
#import "Socket";



// The reactor abstracts the underlying OS multiplexing facility (epoll on Linux,
// kqueue on macOS, IOCP on Windows). We statically `#load` the correct implementation
// at compile time so there is zero runtime overhead for OS checking.
#if OS == .MACOS {
    #load "os/macos.jai";
} else #if OS == .LINUX {
    #load "os/linux.jai";
} else {
    #load "os/windows.jai";
}



// Represents the types of IO events the reactor can monitor.
//
// These map directly to `EPOLLIN`/`EPOLLOUT` on Linux and `EVFILT_READ`/`EVFILT_WRITE`
// on macOS. A fiber will yield until one of these specific events triggers on its descriptor.
Event_Type :: enum {
    READ; WRITE;
}

// The core Event Loop / IO Multiplexer for Feather.
//
// The Reactor pattern inverses the flow of control: instead of the application
// calling `read()` and blocking a system thread, the application registers interest
// in an event (like `Event_Type.READ`) and yields. The Reactor sleeps until the OS
// reports an event, then wakes up the specific fiber waiting for that event.
//
// See [The Reactor Pattern](https://en.wikipedia.org/wiki/Reactor_pattern).
Reactor :: struct {
    using os_data: OS_Reactor;
}



// Initialize the Reactor and its underlying OS structures.
//
// This allocates the kqueue/epoll file descriptor. If this fails, the entire
// asynchronous engine cannot function.
init_reactor :: (r: *Reactor) {
    init_os_reactor(r);
}

// Register a file descriptor and yield the current fiber until an event occurs.
//
// This procedure extracts the `Future` from the current fiber's `context.waker`.
// It tells the OS reactor to monitor `fd` for `type` events, and to wake up
// `fut` when it triggers.
yield :: (r: *Reactor, fd: Socket, type: Event_Type) {
    fut := context.waker.fut;
    yield_os(r, fd, type, fut);
}

// Context-aware yielding.
//
// A convenience wrapper around `yield` that automatically routes the request
// to the specific `Reactor` assigned to the current thread/worker via `context.reactor_index`.
// This avoids needing to pass the reactor pointer explicitly through all HTTP/TCP layers.
// This is the primary injection point connecting socket IO to the async scheduler.
yield_injected :: (fd: Socket, type: Event_Type) {
    yield(*context.feather.reactors[context.reactor_index], fd, type);
}

// The heart of the asynchronous event loop.
//
// `poll_reactor` must be called continuously in a loop by worker threads.
// It performs three critical architectural responsibilities in one pass:
//
// - **Time Management**: It scans the centralized `timers` array. If a timer has expired,
//    it wakes the associated future. Timers require a mutex because fibers from *other*
//    threads may concurrently spawn timers on this thread.
// - **IO Multiplexing**: It invokes `poll_os_reactor(r)` which asks the OS (epoll/kqueue)
//    if any sockets have data ready. It wakes the fibers waiting on those sockets.
// - **Memory Management**: The `Thread_Group` allocates memory for every `add_work`.
//    If we don't call `get_completed_work`, that memory permanently leaks. Since the reactor
//    is the central execution loop, it acts as the garbage-collector for completed tasks.
poll_reactor :: (r: *Reactor) {
    if context.feather {
        // 1. Process timers
        lock(*context.feather.timers_mutex);
        if context.feather.timers.count > 0 {
            now := seconds_since_init() * 1000.0;

            // Iterate backwards to safely remove elements without shifting issues
            for < i: context.feather.timers.count-1..0 {
                timer := context.feather.timers[i];
                if now >= timer.wake_time_ms {
                    wake(timer.fut);
                    array_unordered_remove_by_index(*context.feather.timers, i);
                }
            }
        }
        unlock(*context.feather.timers_mutex);
    }

    // 2. Handle native OS IO multiplexing
    poll_os_reactor(r);

    // 3. Garbage-collect the Thread_Group's completed Work_Entry nodes
    if context.feather && context.feather.exec.group.initted {
        Thread :: #import "Thread";
        Thread.get_completed_work(*context.feather.exec.group);
    }
}

// Destroy the reactor and release native OS handles.
shutdown :: (r: *Reactor) {
    #if OS == .MACOS {
        close(r.kq_fd);
    } else #if OS == .LINUX {
        close(r.epfd);
    }
}
