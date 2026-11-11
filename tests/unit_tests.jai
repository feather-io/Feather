// *** //
// Tests for Feather2 core runtime primitives.
//
// These tests run at compile time via the `#run` directive in `ffte.jai` and `ffte2.jai`.
// They are intended as fast sanity checks on the OS integration layer and core data structures,
// not as substitutes for the runtime integration tests in `ffte.jai`.

#import "Basic";
#import "Socket";
#if OS == .LINUX || OS == .MACOS {
    #import "POSIX";
}

#import,dir "..";

// Entry point that runs all compile-time unit tests.
//
// Wrap calls to this in a `#run` directive to execute at compile time.
run_unit_tests :: () {
    print("--\n");
    print("[ Running Compile Time Tests ]\n");
    print("--\n");

    test_utility_functions();
    test_os_syscalls();
    test_reactor_boot();
    test_fiber_custom_stack_size();

    print("--\n");
    print("[ All Compile Time Tests Passed ]\n");
    print("--\n");
}

// Verify that htons() correctly converts port numbers from host byte order to network byte order.
//
// Network protocols use big-endian byte order. On little-endian hosts (x86/ARM in LE mode),
// htons() swaps the bytes. On big-endian hosts, it is a no-op.
test_utility_functions :: () {
    print("== Test if htons function outputs the correct values\n");
    assert(htons(80) == 20480, "htons(80) failed!");
    assert(htons(8080) == 36895, "htons(8080) failed!");
}

// Verify that basic POSIX syscalls are accessible and return valid results.
//
// This tests three OS integrations: `pipe()` for IPC fd creation, `fcntl()` for setting
// O_NONBLOCK, and `socket()` for TCP socket creation. Failures here indicate a broken
// POSIX import binding rather than a Feather logic bug.
test_os_syscalls :: () {
    print("== Test OS Syscalls are getting fired and handled\n");

    // Pipe creation
    pipe_fds: [2] s32;
    res := pipe(*pipe_fds);
    assert(res == 0, "pipe() failed!");
    assert(pipe_fds[0] > 0 && pipe_fds[1] > 0, "pipe() returned invalid FDs!");

    // Fcntl
    flags := fcntl(pipe_fds[0], F_GETFL, 0);
    res = fcntl(pipe_fds[0], F_SETFL, flags | O_NONBLOCK);
    assert(res == 0, "fcntl() failed!");

    // Socket
    sock := socket(AF_INET, .SOCK_STREAM, 0);
    assert(sock > 0, "socket() failed!");

    close(pipe_fds[0]);
    close(pipe_fds[1]);
    close(sock);
}

// Verify that init_reactor() successfully creates the OS-level IO multiplexer.
//
// On macOS this tests that `kqueue()` returns a valid fd. On Linux it tests `epoll_create1()`.
// A failure here means the OS denied creation of the IO multiplexer, which would make
// the entire async runtime non-functional.
test_reactor_boot :: () {
    print("== Test init_reactor() checks if kqueue/epoll socket is initialized\n");

    reactor: Reactor;
    init_reactor(*reactor);

    #if OS == .MACOS {
        assert(reactor.kq_fd > 0, "Reactor kq_fd is invalid!");
    } else #if OS == .LINUX {
        assert(reactor.epfd > 0, "Reactor epfd is invalid!");
    }

    shutdown(*reactor);
}

// Verify that fibers with non-default stack sizes bypass the Fiber_Stack_Pool and allocate directly.
//
// The pool caches stacks of exactly `stack_pool.stack_size`. Any fiber requesting a different
// size would corrupt the pool if returned. This test ensures the pool bypass path is correct
// and that fiber_destroy() correctly frees directly-allocated stacks without crashing.
test_fiber_custom_stack_size :: () {
    print("== Test Fiber custom stack size bypassing stack pool\n");

    // Mock the runtime context just enough for fiber creation
    runtime: Feather_Runtime;
    init_stack_pool(*runtime.stack_pool);

    push_context,defer_pop; {
        context.feather = *runtime;

        // 1. Test default size (should use pool)
        fiber1 := fiber_create((){}, runtime.stack_pool.stack_size);
        assert(fiber1.stack_size == runtime.stack_pool.stack_size, "Default stack size mismatch!");

        // 2. Test custom size (should bypass pool and allocate directly)
        fiber2 := fiber_create((){}, 64000);
        assert(fiber2.stack_size == 64000, "Custom stack size mismatch!");

        // 3. Destroy both (this is where the segmentation fault would have occurred)
        fiber_destroy(fiber1);
        fiber_destroy(fiber2);
    }

    deinit_stack_pool(*runtime.stack_pool);
}
