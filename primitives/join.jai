#import "Basic";
#import "Atomics";



// Spawn a fiber and await its return value.
//
// This is Feather's primitive for structured concurrency - spawning concurrent work
// and waiting for its result, similar to Go's goroutines with channels or Rust's
// `tokio::spawn().await`.
//
// **Architecture**: The implementation bridges two distinct fiber lifetimes:
// 1. The PARENT fiber calls this function, captures its own `Future` pointer, and yields.
// 2. The CHILD fiber is spawned with `Spawn_Args` passed via `context.workspace`.
//    When the child finishes, it calls `wake(parent_fut)` to reschedule the parent,
//    then atomically sets `done = true`.
//
// The ordering is deliberate: `wake()` fires BEFORE setting `done`. The parent may resume
// briefly, see `done == false`, and yield again - this is safe. The atomic write to `done`
// is the actual memory barrier that guarantees `value` and `success` are visible to the parent.
feather_spawn_await :: (fn: () -> (T, bool), $T: Type, stack_size: s64 = 524288) -> (T, bool) {
    handle: Feather_Join_Handle(T);
    handle.done = false;

    // Capture the waker of the CURRENT fiber so the spawned child can wake us up.
    fut := context.waker.fut;

    args: Spawn_Args(T);
    args.fn = fn;
    args.handle = *handle;
    args.fut = fut;

    new_ctx := context;
    new_ctx.workspace = cast(*void) *args;
    push_context new_ctx {
        fiber_spawn_injected(Spawn_Helper(T).proc, stack_size);
    }

    Basic :: #import "Basic";
    Basic.print("feather_spawn_await: parent yielding, fut=%\n", fut);
    // Yield repeatedly until the child sets done = true.
    while !atomic_read(*handle.done) {
        fiber_yield();
        Basic.print("feather_spawn_await: parent resumed! done=%\n", handle.done);
    }

    return handle.value, handle.success;
}

#scope_file

// Internal join handle shared between the parent and child fiber.
//
// `done` must only be read/written with atomic operations to establish the
// happens-before memory ordering that makes `value` and `success` safe to read.
Feather_Join_Handle :: struct(T: Type) {
    // Atomically set to true by the child after writing value and success.
    done: bool;
    // The return value written by the child fiber.
    value: T;
    // Whether the child fiber succeeded.
    success: bool;
}

// Internal arguments struct passed to the child fiber via `context.workspace`.
Spawn_Args :: struct(T: Type) {
    // The user-provided function to execute in the child fiber.
    fn: () -> (T, bool);
    // Pointer to the join handle on the parent's stack frame.
    handle: *Feather_Join_Handle(T);
    // The parent's Future pointer, used to wake the parent when done.
    fut: *Future;
}

// The child fiber trampoline.
//
// Executes `args.fn`, writes the result into the join handle, wakes the parent,
// and only then sets `done = true` as the final atomic release fence.
Spawn_Helper :: struct(T: Type) {
    proc :: () {
        args := cast(*Spawn_Args(T)) context.workspace;

        val, success := args.fn();

        args.handle.value = val;
        args.handle.success = success;

        fut_to_wake := args.fut;

        // If there is a parent fiber waiting on us, wake it up NOW.
        // It is perfectly safe to wake it before setting `done` because the parent
        // fiber will simply resume, see `done` is false, and immediately yield again.
        if fut_to_wake {
            wake(fut_to_wake);
        }

        // Memory barrier: The parent fiber cannot proceed until this is true.
        // By setting this LAST, we guarantee that `args` (which lives on the parent's stack)
        // and `fut_to_wake` remain completely valid up until this exact instruction.
        atomic_write(*args.handle.done, true);

        Basic :: #import "Basic";
        Basic.print("Spawn_Helper: finished, woke parent fut=%\n", fut_to_wake);
    }
}
