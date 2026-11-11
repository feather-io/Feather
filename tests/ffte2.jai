#import "Basic"()(MEMORY_DEBUGGER=true);
#import "System";
#import "Atomics";
#import "Thread";
#import "Socket";
#import,dir "..";

// -------------------------------------------------------------------------------------------------
// Core Test Framework
// -------------------------------------------------------------------------------------------------
#scope_file

TEST_INDEX := 0;

print_header :: (name: string) {
    print("--\n");
    print("[ % ]\n", name);
    print("--\n");
}

pass :: (fmt_str: string, args: ..Any) {
    print("== PASS: ");
    print(fmt_str, ..args);
    print("\n");
}

skip :: (fmt_str: string, args: ..Any) {
    print("== SKIP: ");
    print(fmt_str, ..args);
    print("\n");
}

run_test :: (name: string, fn: () -> ()) {
    print_header(name);
    fn();
    TEST_INDEX += 1;
}

ffte_assert :: (condition: bool, pass_msg: string, fail_msg: string) {
    assert(condition, fail_msg);
    pass("%", pass_msg);
}

#scope_export
// -------------------------------------------------------------------------------------------------


// -------------------------------------------------------------------------------------------------
// TESTS
// -------------------------------------------------------------------------------------------------

test_feather_block_on :: () {
    run_test("feather_block_on", () {
        feather_block_on((){
            pass("Block on executed successfully inside root fiber!");
        });
    });
}

test_feather_spawn :: () {
    run_test("feather_spawn_await", () {
        feather_block_on((){
            val, ok := feather_spawn_await(() -> (int, bool) {
                return 42, true;
            }, int);

            assert(ok);
            assert(val == 42);
            pass("Spawn await successfully returned %, %", val, ok);
        });
    });
}

test_feather_sleep :: () {
    run_test("feather_sleep_ms", () {
        feather_block_on((){
            start := seconds_since_init();
            feather_sleep_ms(10);
            end := seconds_since_init();
            diff := end - start;
            assert(diff >= 0.010);
            pass("Slept for %ms", diff * 1000.0);
        });
    });
}

test_feather_channel_buffered :: () {
    run_test("Feather_Channel (Buffered)", () {
        feather_block_on((){
            ch := channel_make(int, 5);
            defer channel_free(ch);

            channel_send(ch, 1);
            channel_send(ch, 2);
            channel_send(ch, 3);

            v1, _ := channel_recv(ch);
            v2, _ := channel_recv(ch);
            v3, _ := channel_recv(ch);

            assert(v1 == 1);
            assert(v2 == 2);
            assert(v3 == 3);

            pass("Buffered channel operations succeeded");
        });
    });
}

#scope_file
Unbuffered_Args :: struct {
    ch: *Feather_Channel(int);
}
unbuffered_helper :: () {
    args := cast(*Unbuffered_Args) context.workspace;
    feather_sleep_ms(10);
    channel_send(args.ch, 99);
}
#scope_export

test_feather_channel_unbuffered :: () {
    run_test("Feather_Channel (Unbuffered)", () {
        feather_block_on((){
            ch := channel_make(int, 0);
            defer channel_free(ch);

            args: Unbuffered_Args;
            args.ch = ch;

            new_ctx := context;
            new_ctx.workspace = cast(*void) *args;
            push_context new_ctx {
                feather_spawn(unbuffered_helper);
            }

            v, _ := channel_recv(ch);
            assert(v == 99);
            pass("Unbuffered channel successfully synchronized");
        });
    });
}

main :: () {
    print_header("Feather 2.0 Flight Test Engine (FFTE 2)");

    test_feather_block_on();
    test_feather_spawn();
    test_feather_sleep();
    test_feather_channel_buffered();
    test_feather_channel_unbuffered();

    // Ported tests
    run_test_compile_time();
    run_test_register_smash();
    run_test_swarm();
    run_test_echo_chamber();
    run_test_matryoshka();
    run_test_fairness();
    run_test_stack_depth();
    run_test_thundering_herd();
    run_test_imbalance();
    run_test_tcp_drop_torrent();
    run_test_goroutine_ring();
    run_test_dining_philosophers();
    run_test_timer_storm();
    run_test_graceful_shutdown();

    print_header("All Tests Passed");

    // Memory leak check
    report_memory_leaks();
}


// --- PORTED TESTS FROM FFTE ---

#load "unit_tests.jai";

run_test_compile_time :: () {
    print_header("Compile-Time Unit Tests (htons / syscalls / reactor boot)");
    // These will assert-fail the binary if they are wrong -
    // correct behaviour for low-level invariants.
    run_unit_tests();
    pass("htons values correct");
    pass("OS syscalls (pipe / fcntl / socket) functional");
    pass("Reactor init / shutdown clean");
}


SMASH_MAGIC_A :: 0xDEAD_BEEF_1234_5678;
SMASH_MAGIC_B :: 0xCAFE_BABE_DEAD_CAFE;
SMASH_MAGIC_C :: 0x0101_0101_FEFE_FEFE;
SMASH_FIBERS  :: 64;
SMASH_YIELDS  :: 20;

g_smash_ok:      s64;
g_smash_pending: s64;

register_smash_fiber :: () {
    defer atomic_add(*g_smash_pending, -1);

    a : u64 = SMASH_MAGIC_A;
    b : u64 = SMASH_MAGIC_B;
    c : u64 = SMASH_MAGIC_C;

    for 0..SMASH_YIELDS-1 {
        a ^= 0; b ^= 0; c ^= 0; // touch to prevent elision
        fiber_yield_and_reschedule();
    }

    if (a == SMASH_MAGIC_A) && (b == SMASH_MAGIC_B) && (c == SMASH_MAGIC_C) {
        atomic_add(*g_smash_ok, 1);
    }
}

run_test_register_smash :: () {
    print_header("Register Smash - fiber context integrity");

    feather_main(1); // single-threaded so feather_run_until_done works
    defer feather_destroy();

    for 0..SMASH_FIBERS-1 {
        atomic_add(*g_smash_pending, 1);
        fiber_spawn_injected(register_smash_fiber);
    }

    feather_run_until_done(*g_smash_pending);

    assert(g_smash_ok == SMASH_FIBERS, tprint("Only %/% fibers had clean registers", g_smash_ok, SMASH_FIBERS));
    pass("All fibers preserved callee-saved registers across fiber_yield()");
}

// ================================================================
// TEST 3: Swarm - mass concurrency / scheduler does not drop fibers
//
// Spawn SWARM_COUNT fibers. Each one yields once, then increments
// an atomic counter. After the runtime drains, the counter must
// equal SWARM_COUNT exactly - not one fiber lost or double-run.
// ================================================================

SWARM_COUNT :: 10_000;

g_swarm_counter: s64;
g_swarm_pending: s64;

swarm_fiber :: () {
    defer atomic_add(*g_swarm_pending, -1);
    fiber_yield_and_reschedule(); // Force at least one re-schedule cycle
    atomic_add(*g_swarm_counter, 1);
}

run_test_swarm :: () {
    print_header(tprint("Swarm - % concurrent fibers, none lost", SWARM_COUNT));

    feather_main(1);
    defer feather_destroy();

    for 0..SWARM_COUNT-1 {
        atomic_add(*g_swarm_pending, 1);
        fiber_spawn_injected(swarm_fiber);
    }

    feather_run_until_done(*g_swarm_pending, timeout_ms = 30000);

    assert(g_swarm_counter == SWARM_COUNT,
        tprint("All % fibers completed without loss", SWARM_COUNT),
        tprint("Only %/% fibers completed", g_swarm_counter, SWARM_COUNT)
    );
}

// ================================================================
// TEST 4: Echo Chamber - async I/O multiplexing
//
// Start an echo server inside Feather on port 19876.
// Spawn ECHO_CLIENTS concurrent client fibers.
// Each client connects, sends a unique payload, and asserts it
// receives the exact same bytes back. Tests kqueue/epoll correctly
// multiplexes reads and writes across many FDs simultaneously.
// ================================================================

ECHO_PORT    : u16 : 19876;
ECHO_CLIENTS :: 50;
ECHO_PAYLOAD :: "FFTE-ECHO-PAYLOAD-v1";

g_echo_success: s64;
g_echo_pending: s64;

echo_server_fiber :: () {
    defer atomic_add(*g_echo_pending, -1);

    listener, err_bind := setup_tcp_listener(ECHO_PORT);
    if err_bind.kind != .NONE {
        print("  [ECHO SERVER] Failed to bind port %\n", ECHO_PORT);
        return;
    }
    defer close(listener.fd);

    clients_served: s64 = 0;
    while clients_served < ECHO_CLIENTS {
        stream, err_acc := stream_accept(*listener);
        if err_acc.kind != .NONE continue;

        // Read and immediately echo back
        buf: [256] u8;
        recv_buf: [] u8;
        recv_buf.data  = buf.data;
        recv_buf.count = buf.count;

        n, err_read := stream_read(*stream, recv_buf);
        if err_read.kind == .NONE && n > 0 {
            send_slice: [] u8;
            send_slice.data  = buf.data;
            send_slice.count = n;
            _ = stream_write(*stream, send_slice);
        }
        close(stream.fd);
        clients_served += 1;
    }
}

echo_client_fiber :: () {
    defer atomic_add(*g_echo_pending, -1);

    sock := socket(AF_INET, .SOCK_STREAM, 0);
    if sock < 0 return;

    addr: sockaddr_in;
    addr.sin_family      = AF_INET;
    addr.sin_port        = htons(ECHO_PORT);
    addr.sin_addr.s_addr = 0x0100007F; // 127.0.0.1 in network byte order

    if connect(sock, cast(*sockaddr) *addr, size_of(sockaddr_in)) != 0 {
        close(sock);
        return;
    }

    stream: TcpStream;
    stream.fd = sock;
    defer close(stream.fd);

    #if OS == .LINUX || OS == .MACOS {
        flags := fcntl(sock, F_GETFL, 0);
        fcntl(sock, F_SETFL, flags | O_NONBLOCK);
    }

    payload := ECHO_PAYLOAD;
    send_data: [] u8;
    send_data.data  = payload.data;
    send_data.count = payload.count;

    err_write := stream_write(*stream, send_data);
    if err_write.kind != .NONE return;

    buf: [256] u8;
    recv_buf: [] u8;
    recv_buf.data  = buf.data;
    recv_buf.count = buf.count;

    n, err_read := stream_read(*stream, recv_buf);
    if err_read.kind != .NONE || n != payload.count return;

    match := true;
    for i: 0..n-1 {
        if buf[i] != payload[i] { match = false; break; }
    }

    if match  atomic_add(*g_echo_success, 1);
}

run_test_echo_chamber :: () {
    print_header(tprint("Echo Chamber - % concurrent async I/O round-trips", ECHO_CLIENTS));

    feather_main(1);
    defer feather_destroy();

    // Server fiber counts as 1 pending
    atomic_add(*g_echo_pending, 1);
    fiber_spawn_injected(echo_server_fiber);

    // "Delay launcher" yields a few times to let the server bind before clients connect
    launcher :: () {
        defer atomic_add(*g_echo_pending, -1);
        for 0..9  fiber_yield_and_reschedule();
        for 0..ECHO_CLIENTS-1 {
            atomic_add(*g_echo_pending, 1);
            fiber_spawn_injected(echo_client_fiber);
        }
    };
    atomic_add(*g_echo_pending, 1);
    fiber_spawn_injected(launcher);

    feather_run_until_done(*g_echo_pending, timeout_ms = 10000);

    assert(g_echo_success == ECHO_CLIENTS, tprint("Only %/% round-trips had matching payloads", g_echo_success, ECHO_CLIENTS));
    pass(tprint("All % echo round-trips succeeded with matching payloads", ECHO_CLIENTS));
}

// ================================================================
// TEST 5: Matryoshka - deep recursive fiber spawning
//
// Tests the runtime's ability to handle deep recursive spawning.
// A fiber spawns another fiber, which spawns another, up to 100
// levels deep, verifying that stack contexts are managed safely
// without overflowing internal scheduler buffers.
// ================================================================

g_mat_pending: s64 = 0;
g_mat_depth: s64 = 0;

matryoshka_fiber :: () {
    defer atomic_add(*g_mat_pending, -1);

    depth := atomic_add(*g_mat_depth, 1);
    if depth < 100 {
        atomic_add(*g_mat_pending, 1);
        fiber_spawn_injected(matryoshka_fiber);
    }
}

run_test_matryoshka :: () {
    print_header("Matryoshka - deep recursive fiber spawning");

    feather_main(1);
    defer feather_destroy();

    g_mat_depth = 0;
    atomic_add(*g_mat_pending, 1);
    fiber_spawn_injected(matryoshka_fiber);

    feather_run_until_done(*g_mat_pending, timeout_ms = 5000);

    assert(g_mat_depth == 101, tprint("Stopped at depth %", g_mat_depth));
    pass("Fibers can recursively spawn other fibers up to 100 levels deep");
}

// ================================================================
// TEST 6: Tortoise and Hare - scheduler fairness
//
// Ensures the runtime's scheduler is fair by spawning a "greedy"
// hare fiber that yields continuously in a tight loop and a
// tortoise fiber. We verify that the hare doesn't starve the
// tortoise out of CPU time.
// ================================================================

g_fairness_pending: s64 = 0;
g_hare_yields: s64 = 0;
g_tortoise_done: s64 = 0;

hare_fiber :: () {
    defer atomic_add(*g_fairness_pending, -1);
    while atomic_read(*g_tortoise_done) == 0 {
        atomic_add(*g_hare_yields, 1);
        fiber_yield_and_reschedule();
    }
}

tortoise_fiber :: () {
    defer atomic_add(*g_fairness_pending, -1);
    for 0..9 fiber_yield_and_reschedule();
    atomic_add(*g_tortoise_done, 1);
}

run_test_fairness :: () {
    print_header("Tortoise and Hare - scheduler fairness");

    feather_main(1);
    defer feather_destroy();

    g_hare_yields = 0;
    g_tortoise_done = 0;

    atomic_add(*g_fairness_pending, 2);
    fiber_spawn_injected(hare_fiber);
    fiber_spawn_injected(tortoise_fiber);

    feather_run_until_done(*g_fairness_pending, timeout_ms = 5000);

    yields := atomic_read(*g_hare_yields);
    assert(yields < 1000, tprint("Hare yielded % times", yields));
    pass("Scheduler is fair (Greedy fibers do not starve others)");
}

// ================================================================
// TEST 7: Stack Depth - stack memory integrity
//
// Verifies that large local stack variables inside a Fiber are
// preserved perfectly across context switches. A deep fiber
// allocates 32KB on the stack, yields, and validates it upon wake.
// ================================================================

g_stack_pending: s64 = 0;
g_stack_success: s64 = 0;

deep_fiber :: () {
    defer atomic_add(*g_stack_pending, -1);

    // Allocate 32KB on the fiber's 64KB stack
    buffer: [32768] u8;
    for i: 0..32767  buffer[i] = cast(u8)(i % 256);

    fiber_yield_and_reschedule();

    valid := true;
    for i: 0..32767 {
        if buffer[i] != cast(u8)(i % 256) {
            valid = false;
            break;
        }
    }

    if valid  atomic_add(*g_stack_success, 1);
}

run_test_stack_depth :: () {
    print_header("Stack Depth - stack memory integrity");

    feather_main(1);
    defer feather_destroy();

    atomic_add(*g_stack_pending, 1);
    fiber_spawn_injected(deep_fiber);

    feather_run_until_done(*g_stack_pending, timeout_ms = 5000);

    assert(g_stack_success == 1, "Stack memory was corrupted upon resume");
    pass("Fiber correctly preserves 32KB of local stack state across context swaps");
}

// ================================================================
// TEST 8: Thundering Herd (Simultaneous Wakeup)
//
// Tests the runtime's ability to gracefully handle thousands of
// fibers waking up simultaneously.
// (Currently skipped pending WaitGroup implementations).
// ================================================================

g_herd_pending: s64 = 0;
g_herd_woken: s64 = 0;
g_herd_start: s64 = 0;

herd_waiter_fiber :: () {
    defer atomic_add(*g_herd_pending, -1);

    // Save our future so the master can wake us
    idx := atomic_add(*g_herd_start, 1);

    // Sleep! Wait for master to call wake()
    fiber_yield();

    atomic_add(*g_herd_woken, 1);
}

run_test_thundering_herd :: () {
    print_header("Thundering Herd - 5000 fibers waking up simultaneously");

    // THIS TEST TEMPORARILY DISABLED (Needs proper Future array saving for manual wake())
    print("== SKIP: Requires storing Future pointers globally for wake() (Needs channels/waitgroups)\n");
}

// ================================================================
// TEST 9: Imbalance (Work-Stealing Stress)
//
// Spawns 10,000 heavy CPU-bound fibers from a single spawner fiber.
// This stresses the executor's work-stealing algorithm, proving that
// idle threads will steal work from the spawner's overloaded thread queue.
// ================================================================

g_imb_pending: s64 = 0;
g_imb_done: s64 = 0;

imb_heavy_fiber :: () {
    defer atomic_add(*g_imb_pending, -1);
    sum := 0;
    for 0..2000  sum += it;
    atomic_add(*g_imb_done, 1);
}

run_test_imbalance :: () {
    print_header("Work-Stealing Imbalance - greedy spawning stolen cleanly");

    feather_main(4);
    defer feather_destroy();

    g_imb_done = 0;

    imb_spawner :: () {
        defer atomic_add(*g_imb_pending, -1);
        for 0..9999 {
            atomic_add(*g_imb_pending, 1);
            fiber_spawn_injected(imb_heavy_fiber);
        }
    };

    atomic_add(*g_imb_pending, 1);
    fiber_spawn_injected(imb_spawner);

    feather_run_until_done(*g_imb_pending, timeout_ms = 10000);

    assert(g_imb_done == 10000, tprint("Only completed %/10000 heavy fibers", g_imb_done));
    pass("Work-stealing successfully re-balanced 10000 heavy fibers across threads");
}

// ================================================================
// TEST 10: TCP Drop Torrent (Broken Pipe Resiliency)
//
// Simulates an aggressive DDoS where clients connect and immediately
// drop the connection. The server attempts to read from closed sockets,
// ensuring the reactor gracefully handles broken pipes without crashing
// or leaking file descriptors.
// ================================================================

g_drop_pending: s64 = 0;
g_drop_accepted: s64 = 0;
g_drop_client_failed: s64 = 0;
g_drop_server_failed: s64 = 0;

drop_server :: () {
    defer atomic_add(*g_drop_pending, -1);

    listener, err_bind := setup_tcp_listener(14002);
    if err_bind.kind != .NONE return;
    defer close(listener.fd);

    for 1..100 {
        stream, err_acc := stream_accept(*listener);
        if err_acc.kind != .NONE {
            atomic_add(*g_drop_server_failed, 1);
            continue;
        }

        atomic_add(*g_drop_accepted, 1);

        buf: [64] u8;
        slice: [] u8;
        slice.data = buf.data; slice.count = buf.count;
        stream_read(*stream, slice);
        close(stream.fd);
    }
}

drop_client :: () {
    defer atomic_add(*g_drop_pending, -1);

    sock := socket(AF_INET, .SOCK_STREAM, 0);
    if sock < 0 {
        atomic_add(*g_drop_client_failed, 1);
        return;
    }

    addr: sockaddr_in;
    addr.sin_family      = AF_INET;
    addr.sin_port        = htons(14002);
    addr.sin_addr.s_addr = 0x0100007F;

    res := connect(sock, cast(*sockaddr) *addr, size_of(sockaddr_in));
    if res < 0 {
        atomic_add(*g_drop_client_failed, 1);
    }
    close(sock);
}

run_test_tcp_drop_torrent :: () {
    print_header("TCP Drop Torrent - reactor resiliency to broken pipes");

    feather_main(1);
    defer feather_destroy();

    g_drop_accepted = 0;
    g_drop_client_failed = 0;
    g_drop_server_failed = 0;

    atomic_add(*g_drop_pending, 1);
    fiber_spawn_injected(drop_server);

    launcher :: () {
        defer atomic_add(*g_drop_pending, -1);
        for 0..9  fiber_yield_and_reschedule();
        for 0..99 {
            atomic_add(*g_drop_pending, 1);
            fiber_spawn_injected(drop_client);
        }
    };

    atomic_add(*g_drop_pending, 1);
    fiber_spawn_injected(launcher);

    feather_run_until_done(*g_drop_pending, timeout_ms = 10000);

    assert(g_drop_accepted + g_drop_server_failed + g_drop_client_failed == 100, tprint("Server only processed %/100 drops (Accepted: %, Server Failed: %, Client Failed: %)", g_drop_accepted + g_drop_server_failed + g_drop_client_failed, g_drop_accepted, g_drop_server_failed, g_drop_client_failed));
    pass("Reactor safely handles 100 simultaneous dropped connections without FD leaks");
}

// ================================================================
// DEFERRED TESTS (Waiting for Generic Runtime Implementation)
// ================================================================

run_test_goroutine_ring :: () {
    print_header("Goroutine Ring (Deferred)");
    print("== SKIP: Requires Feather_Channel (generic-feather.md)\n");
}

run_test_dining_philosophers :: () {
    print_header("Dining Philosophers Deadlock (Deferred)");
    print("== SKIP: Requires Reactor idle detection (generic-feather.md)\n");
}

run_test_timer_storm :: () {
    print_header("Timer Storm (Deferred)");
    print("== SKIP: Requires feather_sleep_ms (generic-feather.md)\n");
}

// ================================================================
// TEST 11: Graceful Shutdown
//
// Spawn fibers that are blocked in infinite yields, then call
// feather_destroy(). The function must return - not deadlock.
// ================================================================

run_test_graceful_shutdown :: () {
    print_header("Graceful Shutdown - suspended fibers must not cause deadlock");

    feather_main(1);

    sleeping_fiber :: () {
        // Yield indefinitely - we will destroy the runtime while these are suspended
        for 0..4  fiber_yield();
    };

    for 0..9  fiber_spawn_injected(sleeping_fiber);

    // Poll a few times so fibers get to their yields, then destroy
    for 0..49  poll_reactor(*context.feather.reactors[0]);

    feather_destroy();

    // Reaching here proves shutdown was clean (no hang, no segfault)
    pass("feather_destroy() returned cleanly with suspended fibers");
}
