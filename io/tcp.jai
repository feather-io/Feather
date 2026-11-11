#import "Basic";
#import "Socket";
#if OS == .LINUX || OS == .MACOS {
    #import "POSIX";
}



// A TCP Listener bound to a specific port.
//
// This struct wraps a native OS socket descriptor configured for listening
// to incoming TCP connections. It forms the base of the server acceptor architecture.
TcpListener :: struct {
    // The native OS socket file descriptor.
    fd: Socket;
}

// Bind to a local port and start listening for incoming TCP connections.
//
// This procedure creates a socket, sets `SO_REUSEADDR` to prevent "address already in use"
// errors during rapid server restarts, binds it to all interfaces (0.0.0.0), and starts
// listening.
//
// **OS Mechanics**: On POSIX systems, it immediately configures the socket to be
// non-blocking via `O_NONBLOCK`. This ensures that subsequent calls to `accept()` will
// return instantly with `EWOULDBLOCK` instead of pausing the OS thread, which is mandatory
// for the async runtime.
setup_tcp_listener :: (port: u16) -> (listener: TcpListener, err: Error) {
    listener: TcpListener;
    listener.fd = socket(AF_INET, .SOCK_STREAM, 0);
    if listener.fd == -1 return listener, ERR_TCP_SOCKET;

    optval: s32 = 1;
    setsockopt(listener.fd, SOL_SOCKET, SO_REUSEADDR, cast(*void)*optval, size_of(s32));

    addr: sockaddr_in;
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    addr.sin_addr.s_addr = 0;

    if bind(listener.fd, cast(*sockaddr)*addr, size_of(sockaddr_in)) == -1 return listener, ERR_STREAM_ACCEPT;
    if listen(listener.fd, 1024) == -1 return listener, ERR_STREAM_ACCEPT;

    #if OS == .LINUX || OS == .MACOS {
        flags := fcntl(listener.fd, F_GETFL, 0);
        if fcntl(listener.fd, F_SETFL, flags | O_NONBLOCK) == -1 {
            return listener, ERR_TCP_NONBLOCK;
        }
    }

    return listener, ERR_NONE;
}

// A TCP Stream representing an active connection.
//
// This struct wraps a native OS socket descriptor for a connected TCP socket.
// It is used for both outgoing client connections and incoming accepted client connections.
TcpStream :: struct {
    // The native OS socket file descriptor.
    fd: Socket;
}

// Close the TCP stream and release the underlying OS resources.
//
// This invokes the native `close()` or `closesocket()` depending on the OS platform.
close_stream :: (stream: *TcpStream) {
    #if OS == .LINUX || OS == .MACOS {
        close(stream.fd);
    } else {
        closesocket(stream.fd);
    }
}


//
// ARCHITECTURE NOTE: Transparent Asynchrony
// The following procedures are "runtime-aware". They check `context.feather` to see if
// they are being executed inside the Feather async runtime.
//
// - **Blocking Mode**: If outside the runtime, they execute standard, blocking POSIX socket calls.
// - **Async Mode**: If inside the runtime, they execute in a loop: they attempt the IO, and if
//   the OS returns `EWOULDBLOCK`, they suspend the current fiber and yield control back to the
//   Reactor. This achieves the performance of async/await with the exact syntax and stack-trace
//   simplicity of synchronous code.

// Connect to a remote TCP endpoint.
//
// Initiates a TCP connection to the provided `sockaddr_in`.
//
// **OS Mechanics**: In async mode, this sets the socket to non-blocking and calls `connect()`.
// Since a TCP 3-way handshake takes time, `connect()` will immediately return `-1` with
// `errno == EINPROGRESS`. We then ask the Reactor to wake us up when the socket becomes
// writable (`.WRITE`), which signifies that the handshake has completed. We then check
// `SO_ERROR` to ensure the handshake actually succeeded.
stream_connect :: (addr: *sockaddr_in) -> (stream: TcpStream, err: Error) {
    stream: TcpStream;
    stream.fd = socket(AF_INET, .SOCK_STREAM, 0);
    if stream.fd == -1 return stream, ERR_TCP_SOCKET;

    if !context.feather {
        res := connect(stream.fd, cast(*sockaddr)addr, size_of(sockaddr_in));
        if res == -1 return stream, ERR_STREAM_CONNECT;
        return stream, ERR_NONE;
    }

    // Async mode
    #if OS == .LINUX || OS == .MACOS {
        flags := fcntl(stream.fd, F_GETFL, 0);
        if fcntl(stream.fd, F_SETFL, flags | O_NONBLOCK) == -1 {
            return stream, ERR_TCP_NONBLOCK;
        }
    }

    res := connect(stream.fd, cast(*sockaddr)addr, size_of(sockaddr_in));
    if res == -1 {
        #if OS == .LINUX || OS == .MACOS {
            err_code := errno();
            if err_code == EINPROGRESS {
                yield_injected(stream.fd, .WRITE);
                fiber_yield();

                so_err: s32;
                len: socklen_t = size_of(s32);
                getsockopt(stream.fd, SOL_SOCKET, SO_ERROR, cast(*void)*so_err, *len);
                if so_err != 0 {
                    return stream, ERR_STREAM_CONNECT;
                }
                return stream, ERR_NONE;
            }
        }
        return stream, ERR_STREAM_CONNECT;
    }
    return stream, ERR_NONE;
}

// Accept a new incoming TCP connection.
//
// In async mode, this operation yields the current fiber until a new client connects.
stream_accept :: (listener: *TcpListener) -> (stream: TcpStream, err: Error) {
    if !context.feather {
        client_fd := accept(listener.fd, null, null);
        if client_fd == -1 return .{}, ERR_STREAM_ACCEPT;

        stream: TcpStream;
        stream.fd = client_fd;
        return stream, ERR_NONE;
    }

    while true {
        stream, status := accept_async(listener);
        if status == .PENDING {
            fiber_yield();
            continue;
        } else if status == .ERROR || status == .EOF {
            return .{}, ERR_STREAM_ACCEPT;
        }
        return stream, ERR_NONE;
    }
    return .{}, ERR_STREAM_ACCEPT;
}

// Read bytes from the TCP stream into the provided buffer.
//
// In async mode, this operation uses a `while true` loop to repeatedly try reading.
// If the underlying OS buffers are empty, `read_async` returns `.PENDING`, which
// puts the fiber to sleep until the Reactor wakes it up when data arrives.
stream_read :: (stream: *TcpStream, buffer: [] u8) -> (bytes_read: s64, err: Error) {
    if !context.feather {
        bytes := recv(stream.fd, buffer.data, cast,no_check(u64) buffer.count, 0);
        if bytes == -1 return 0, ERR_STREAM_READ;
        if bytes == 0 return 0, ERR_NONE; // EOF
        return bytes, ERR_NONE;
    }

    while true {
        bytes_read, status := read_async(stream, buffer);
        if status == .PENDING {
            fiber_yield();
            continue;
        } else if status == .ERROR {
            return 0, ERR_STREAM_READ;
        } else if status == .EOF {
            return 0, ERR_NONE;
        }
        return bytes_read, ERR_NONE;
    }
    return 0, ERR_STREAM_READ;
}

// Write bytes to the TCP stream.
//
// **OS Mechanics**: Native TCP sockets can write *partial data* due to OS buffer limits
// or network congestion. This architecture ensures we loop over `offset < data.count`
// repeatedly, shifting the slice pointer forward, until every single byte of the buffer
// is successfully dispatched. If the OS socket buffer fills up, it yields the fiber.
stream_write :: (stream: *TcpStream, data: [] u8) -> Error {
    if !context.feather {
        bytes := send(stream.fd, data.data, cast,no_check(u64) data.count, 0);
        if bytes == -1 return ERR_STREAM_WRITE;
        return ERR_NONE;
    }

    offset := 0;
    while offset < data.count {
        slice_to_write: [] u8;
        slice_to_write.data = data.data + offset;
        slice_to_write.count = data.count - offset;

        bytes_written, status := write_async(stream, slice_to_write);

        if status == .PENDING {
            fiber_yield();
            continue;
        } else if status == .ERROR || status == .EOF {
            return ERR_STREAM_WRITE;
        }

        offset += bytes_written;
    }

    return ERR_NONE;
}

// Accept a new TCP connection asynchronously.
//
// Returns a `.PENDING` state if there are no new connections available (which is
// indicated by `EWOULDBLOCK` or `EAGAIN` from the OS), signaling the caller to yield.
accept_async :: (listener: *TcpListener) -> (stream: TcpStream, status: Poll_State) {
    client_fd := accept(listener.fd, null, null);

    if client_fd == -1 {
        #if OS == .LINUX || OS == .MACOS {
            err := errno();
            if err == EWOULDBLOCK || err == EAGAIN {
                yield_injected(listener.fd, .READ);
                return .{}, .PENDING;
            }
        }
        return .{}, .ERROR;
    }

    #if OS == .LINUX || OS == .MACOS {
        flags := fcntl(client_fd, F_GETFL, 0);
        fcntl(client_fd, F_SETFL, flags | O_NONBLOCK);

        // TCP_NODELAY disables Nagle's algorithm, forcing packets to send immediately.
        // This is critical for low-latency web server responses.
        optval: s32 = 1;
        IPPROTO_TCP :: 6;
        setsockopt(client_fd, IPPROTO_TCP, TCP_NODELAY, cast(*void)*optval, size_of(s32));
    }

    stream: TcpStream;
    stream.fd = client_fd;
    return stream, .READY;
}


#scope_module

// Read bytes from the TCP stream asynchronously.
//
// Returns `.PENDING` if the operation would block.
read_async :: (stream: *TcpStream, buffer: [] u8) -> (bytes_read: s64, status: Poll_State) {
    bytes := recv(stream.fd, buffer.data, cast,no_check(u64) buffer.count, 0);

    if bytes == -1 {
        #if OS == .LINUX || OS == .MACOS {
            err := errno();
            if err == EWOULDBLOCK || err == EAGAIN {
                yield_injected(stream.fd, .READ);
                return 0, .PENDING;
            }
        }
        return 0, .ERROR;
    }

    if bytes == 0 return 0, .EOF;
    return bytes, .READY;
}

// Write bytes to the TCP stream asynchronously.
//
// Returns `.PENDING` if the OS socket buffer is full and would block.
write_async :: (stream: *TcpStream, data: [] u8) -> (bytes_written: s64, status: Poll_State) {
    bytes := send(stream.fd, data.data, cast,no_check(u64) data.count, 0);

    if bytes == -1 {
        #if OS == .LINUX || OS == .MACOS {
            err := errno();
            if err == EWOULDBLOCK || err == EAGAIN {
                yield_injected(stream.fd, .WRITE);
                return 0, .PENDING;
            }
        }
        return 0, .ERROR;
    }
    return bytes, .READY;
}
