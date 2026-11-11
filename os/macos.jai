#import "POSIX";

libc :: #system_library "libc";

// Allocate a new kernel event queue and return its file descriptor.
//
// `kqueue` is macOS's IO multiplexer - the equivalent of Linux's `epoll`. The
// fundamental difference in philosophy is that `kqueue` is **event-based**:
// you describe what you are *interested in* (a changeset), and the kernel
// tells you what *actually happened* (fired events). Both the registration of
// interests and the retrieval of fired events happen through a single syscall
// (`kevent`), unlike Linux where `epoll_ctl` and `epoll_wait` are separate.
//
// See: https://www.freebsd.org/cgi/man.cgi?query=kqueue
kqueue :: () -> s32 #foreign libc;

// Submit changes to the event queue and/or retrieve fired events.
//
// This single syscall serves two purposes controlled by its arguments:
//   - **Registration**: pass a non-null `changelist` with `nchanges > 0` to
//     register or modify interest in events.
//   - **Retrieval**: pass a non-null `eventlist` with `nevents > 0` to collect
//     events the kernel has already fired.
//
// Both modes can be combined in a single call. In Feather's reactor, we always
// use them **separately** - one call to register, one call to retrieve - for
// clarity. The `timeout` on the retrieval call controls the maximum time the
// kernel will block waiting for events.
kevent :: (kq: s32, changelist: *Kevent, nchanges: s32, eventlist: *Kevent,
    nevents: s32, timeout: *timespec) -> s32 #foreign libc;


// The kernel's representation of a single registered event interest or a
// fired event notification.
//
// When used as a **changeset** (passed to `kevent` for registration), the
// fields describe what the caller wants to watch. When returned as a **fired
// event**, the kernel fills the same struct with details of what occurred.
//
// - `ident`: the identity of the event source (e.g., a file descriptor for IO).
// - `filter`: the class of event to watch (`EVFILT_READ`, `EVFILT_WRITE`, etc.).
// - `flags`: behaviour modifiers (e.g., `EV_ADD`, `EV_ONESHOT`).
// - `fflags`: filter-specific flags.
// - `data`: filter-specific data (e.g., bytes available to read).
// - `udata`: **opaque user pointer** - the kernel stores and returns this
//   unchanged. Feather uses it to carry the `*Future` associated with each IO
//   wait, so the reactor can resume the correct fiber when an event fires.
Kevent :: struct {
    ident:  u64;
    filter: s16;
    flags:  u16;
    fflags: u32;
    data:   s64;
    udata:  *void;
}

EVFILT_READ  : s16 : -1;
EVFILT_WRITE : s16 : -2;

EV_ADD    : u16 : 0x0001;
EV_ENABLE : u16 : 0x0004;

// Auto-remove this interest from the kqueue after it fires once.
//
// This flag is **critically important** to Feather's correctness. Without it,
// a single edge (e.g., a socket becoming readable) would make `kevent`
// return the same event on **every subsequent poll**, creating a busy-spin
// that would consume 100% CPU. `EV_ONESHOT` instructs the kernel to
// atomically fire the event and then immediately delete the interest, so the
// reactor is only woken up once per explicitly registered wait - matching
// exactly the `yield_os` / `poll_os_reactor` contract.
EV_ONESHOT : u16 : 0x0010;





// The OS-specific state for Feather's Reactor on macOS.
//
// Holds the single kqueue file descriptor that backs the entire IO multiplexer
// for this reactor instance. All `kevent` calls on this reactor funnel through
// this one descriptor.
OS_Reactor :: struct {
    kq_fd: s32;
}

// Create the underlying kqueue and store its fd in `r`.
init_os_reactor :: (r: *Reactor) {
    r.kq_fd = kqueue();
}

// Register a one-shot IO interest and associate `fut` with it, then return
// immediately without blocking.
//
// This is the macOS implementation of the Reactor Pattern's **interest
// registration** step. It does not wait for the IO to complete. Instead, it
// tells the kernel "wake me up the next time `fd` is ready for `type`", and
// parks the caller's fiber. `poll_os_reactor` will later retrieve the fired
// event and call `execute_future_inline(fut)` to resume execution.
//
// The `udata` field of the `Kevent` carries `fut` through the kernel
// unchanged, so when the event fires we can identify which `*Future` to
// resume without any additional lookup table.
//
// `EV_ONESHOT` ensures the interest is automatically removed after firing,
// preventing a spin-loop on repeated polls.
yield_os :: (r: *Reactor, fd: Socket, type: Event_Type, fut: *Future) {
    event: Kevent;
    event.ident = cast(u64) fd;
    event.filter = ifx type == .READ then EVFILT_READ else EVFILT_WRITE;
    event.flags = EV_ADD | EV_ENABLE | EV_ONESHOT;
    event.udata = cast(*void) fut;
    kevent(r.kq_fd, *event, 1, null, 0, null);
}

// Drain all fired IO events from the kqueue and resume their associated fibers.
//
// This is the Reactor's **event dispatch** step, called on every iteration of
// the Feather scheduler loop. Its responsibilities are:
//   1. **Collect fired events**: call `kevent` in retrieval mode, blocking for
//      at most 5 ms. The 5 ms timeout is deliberate - it keeps the reactor
//      from busy-spinning on an idle queue while still allowing the scheduler
//      to perform graceful shutdown checks and other bookkeeping at a
//      reasonable cadence.
//   2. **Resume fibers**: for each fired event, cast `udata` back to `*Future`
//      and call `execute_future_inline`, which continues the suspended fiber
//      from the point it yielded.
//
// Because every registered interest uses `EV_ONESHOT`, each `*Future` pointer
// in `udata` is guaranteed to appear at most once per poll cycle.
poll_os_reactor :: (r: *Reactor) {
    events: [256] Kevent;

    timeout: timespec;
    timeout.tv_nsec = 5_000_000; // 5ms - prevents busy-spin, allows shutdown checks

    num_events := kevent(r.kq_fd, null, 0, events.data, events.count, *timeout);

    if num_events <= 0  return;

    for i : 0..num_events-1 {
        ev := events[i];
        fut := cast(*Future) ev.udata;
        if !fut  continue;

        execute_future_inline(fut);
    }
}



// The exact callee-saved register state for a fiber context on Apple Silicon
// (ARM64).
//
// When `fiber_swap` switches execution from one fiber to another, it saves the
// **callee-saved** registers of the outgoing fiber into this struct and
// restores the same set from the incoming fiber's struct. Only callee-saved
// registers need to be preserved - the caller-saved registers are, by
// definition, already discarded at every call site.
//
// Per the **ARM64 ABI** (Procedure Call Standard for Arm 64-bit Architecture,
// §6.1.2):
//   - `sp`      - Stack Pointer; the top of the fiber's private stack.
//   - `pc`      - Program Counter; stored in the link register `x30`, which
//                 holds the return address the fiber will jump to on resume.
//   - `fp`      - Frame Pointer (`x29`); required by the ABI for stack
//                 unwinding and debugger backtraces.
//   - `x19–x28` - Ten callee-saved general-purpose registers.
//   - `d8–d15`  - Eight callee-saved SIMD/floating-point registers.
//
// The layout here must match the offsets used by the `fiber_swap` assembly
// stub exactly - any mismatch corrupts the fiber's execution state.
Register_Mac_ARM64 :: struct {
    sp: u64;           // Stack Pointer
    pc: u64;           // Program Counter (link register x30)
    fp: u64;           // Frame Pointer (x29)
    x19_x28: [10] u64; // Callee-saved general purpose registers
    d8_d15: [8] float64; // Callee-saved SIMD/float registers
}
