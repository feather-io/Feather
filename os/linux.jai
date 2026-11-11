#import "Linux";
#import "POSIX";




// The OS-specific state for Feather's Reactor on Linux.
//
// Holds the single epoll file descriptor that backs the entire IO multiplexer
// for this reactor instance. All `epoll_ctl` and `epoll_wait` calls on this
// reactor funnel through this one descriptor.
OS_Reactor :: struct {
    epfd: s32;
}

// Create the underlying epoll instance and store its fd in `r`.
init_os_reactor :: (r: *Reactor) {
    r.epfd = epoll_create1(0);
}

// Register a one-shot IO interest and associate `fut` with it, then return
// immediately without blocking.
//
// This is the Linux implementation of the Reactor Pattern's **interest
// registration** step. It mirrors `yield_os` on macOS but uses epoll instead
// of kqueue.
//
// **`EPOLLONESHOT` - why it matters**: just like `EV_ONESHOT` on kqueue,
// `EPOLLONESHOT` instructs the kernel to auto-disable the fd's interest after
// it fires once. Without it, `epoll_wait` would return the same fd as ready
// on every subsequent call, causing a spin-loop that burns 100% CPU.
//
// **MOD-then-ADD fallback - the idiomatic epoll pattern**: `epoll_ctl(MOD)`
// is used first because, if the fd was already registered (e.g., a prior
// `EPOLLONESHOT` fired and the interest was *disabled* rather than *deleted*),
// MOD is the only valid operation. However, if the fd has *never* been added
// to this epoll instance, MOD returns `ENOENT`. In that case we fall through
// to `epoll_ctl(ADD)`. This try-MOD-then-ADD idiom avoids a separate "is it
// registered?" bookkeeping table and is the standard pattern in production
// Linux async runtimes.
//
// `event.data.ptr` carries `fut` through the kernel unchanged (it is a union
// field), so `poll_os_reactor` can recover the `*Future` without a lookup.
yield_os :: (r: *Reactor, fd: Socket, type: Event_Type, fut: *Future) {
    event: epoll_event;
    event.events = EPOLLIN | EPOLLONESHOT;
    if type == .WRITE {
        event.events = EPOLLOUT | EPOLLONESHOT;
    }
    // event.data is a union; store the Future pointer in its ptr field so
    // poll_os_reactor can cast it back without a side-table lookup.
    event.data.ptr = cast(*void) fut;

    if epoll_ctl(r.epfd, .MOD, cast(s32) fd, *event) == -1 {
        err := errno();
        // ENOENT means this fd has never been added to the epoll instance.
        // Fall back to ADD to register it for the first time.
        if err == ENOENT {
            epoll_ctl(r.epfd, .ADD, cast(s32)fd, *event);
        }
    }
}

// Drain all fired IO events from the epoll instance and resume their
// associated fibers.
//
// This is the Reactor's **event dispatch** step, called on every iteration of
// the Feather scheduler loop. Its responsibilities are:
//   1. **Collect fired events**: call `epoll_wait` blocking for at most 5 ms.
//      The 5 ms timeout matches `macos.jai` and is deliberate - it prevents
//      busy-spinning on an idle queue while still allowing the scheduler to
//      perform graceful shutdown checks at a reasonable cadence.
//   2. **Resume fibers**: for each fired event, cast `data.ptr` back to
//      `*Future` and call `execute_future_inline`, which continues the
//      suspended fiber from the point it called `yield_os`.
//
// Because every registered interest uses `EPOLLONESHOT`, each `*Future`
// pointer is guaranteed to appear at most once per poll cycle.
poll_os_reactor :: (r : *Reactor) {
    events : [256] epoll_event;

    // 5ms timeout to match macos.jai graceful shutdown
    num_events := epoll_wait(r.epfd, events.data, cast(s32) events.count, 5);

    if num_events <= 0 return;

    for i : 0..num_events-1 {
        ev := events[i];
        fut := cast(*Future) ev.data.ptr;
        if !fut continue;

        execute_future_inline(fut);
    }
}



// The exact callee-saved register state for a fiber context on x86-64 Linux.
//
// When `fiber_swap` switches execution from one fiber to another, it saves the
// **callee-saved** registers of the outgoing fiber into this struct and
// restores the same set from the incoming fiber's struct.
//
// Per the **System V AMD64 ABI** (the calling convention on Linux x86-64):
//   - `sp` (+0)  - Stack Pointer (`rsp`); the top of the fiber's private stack.
//   - `bp` (+8)  - Base/Frame Pointer (`rbp`); required by the ABI for stack
//                  unwinding and debugger backtraces.
//   - `bx` (+16) - `rbx`; callee-saved general-purpose register.
//   - `r12` (+24) through `r15` (+48) - four additional callee-saved
//                  general-purpose registers mandated by the ABI.
//
// Caller-saved registers (`rax`, `rcx`, `rdx`, `rsi`, `rdi`, `r8`–`r11`)
// are **not** stored here - they are already discarded at every call boundary
// and need not survive a context switch.
//
// The `+N` byte offsets in the comments must match the offsets used by the
// `fiber_swap` assembly stub exactly, any mismatch corrupts the fiber's
// execution state.
Register_X64 :: struct {
    sp:  u64; // +0   rsp - Stack Pointer
    bp:  u64; // +8   rbp - Frame/Base Pointer
    bx:  u64; // +16  rbx - callee-saved general
    r12: u64; // +24  r12 - callee-saved general
    r13: u64; // +32  r13 - callee-saved general
    r14: u64; // +40  r14 - callee-saved general
    r15: u64; // +48  r15 - callee-saved general
}
