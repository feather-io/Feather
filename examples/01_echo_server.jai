#import "Basic";
#import "Socket";
#if OS == .LINUX || OS == .MACOS {
    #import "POSIX";
}
#import,dir "..";

// Control flag for graceful shutdown
KEEP_RUNNING := true;

handle_sigint :: (sig: s32) #c_call {
    KEEP_RUNNING = false;
}

// Handler fiber running concurrently for each accepted client connection.
client_handler :: () {
    fd := cast(Socket) context.workspace;
    stream: TcpStream;
    stream.fd = fd;
    defer close(stream.fd);

    print("[+] Client connected (FD %)\n", fd);

    buf: [1024] u8;
    while KEEP_RUNNING {
        slice: [] u8;
        slice.data = buf.data;
        slice.count = buf.count;

        // stream_read is context-aware: if no data is ready, it automatically yields
        // this fiber to the Reactor and lets other fibers/connections execute!
        bytes_read, err := stream_read(*stream, slice);
        if err.kind != .NONE || bytes_read == 0 {
            // Client closed connection (EOF) or error occurred
            break;
        }

        // Echo the received bytes back to the client
        out_slice: [] u8;
        out_slice.data = buf.data;
        out_slice.count = bytes_read;

        err_write := stream_write(*stream, out_slice);
        if err_write.kind != .NONE {
            break;
        }
    }

    print("[-] Client disconnected (FD %)\n", fd);
}

// Master acceptor fiber that listens for incoming TCP connections.
acceptor_fiber :: () {
    PORT : u16 = 8080;
    listener, err := setup_tcp_listener(PORT);
    if err.kind != .NONE {
        print("[!] Failed to bind TCP listener to port %: %\n", PORT, err);
        KEEP_RUNNING = false;
        return;
    }
    defer close(listener.fd);

    print("============================================\n");
    print(" 🪶 Feather Async TCP Echo Server\n");
    print(" Listening on: 0.0.0.0:%\n", PORT);
    print(" Connect with: nc 127.0.0.1 %\n", PORT);
    print(" Press Ctrl+C to stop.\n");
    print("============================================\n");

    while KEEP_RUNNING {
        stream, err_acc := stream_accept(*listener);
        if err_acc.kind != .NONE {
            if !KEEP_RUNNING break;
            continue;
        }

        // Spawn a dedicated, lightweight fiber for this client
        new_ctx := context;
        new_ctx.workspace = cast(*void) stream.fd;
        push_context new_ctx {
            feather_spawn(client_handler);
        }
    }

    print("[*] Acceptor loop terminated.\n");
}

main :: () {
    #if OS == .LINUX || OS == .MACOS {
        action: sigaction_t;
        sigemptyset(*action.sa_mask);
        action.sa_handler = handle_sigint;
        sigaction(SIGINT, *action, null);
    }

    // 1. Initialize Feather runtime with multi-core reactor threads
    feather_main();
    defer feather_destroy();

    // 2. Spawn the acceptor fiber
    feather_spawn(acceptor_fiber);

    // 3. Drive the event loop on the main thread until Ctrl+C is caught
    feather_run(*KEEP_RUNNING);

    print("Server cleanly shut down.\n");
}
