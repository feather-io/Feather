Error :: struct {
    kind: Error_Kind;
    message: string;
}

Error_Kind :: enum u16 {
    NONE;

    // Feather Errors
    STREAM_CONNECT_FAILED;
    STREAM_READ_FAILED;
    STREAM_WRITE_FAILED;
    STREAM_ACCEPT_FAILED;
    TCP_SOCKET_CREATION_FAILED;
    TCP_SET_NONBLOCK_FAILED;

    // Falcon Errors
    URL_PARSE_FAILED;
    DNS_RESOLUTION_FAILED;
    MALFORMED_SERVER_RESPONSE;
}

// Global predefined errors for common cases
ERR_NONE :: Error.{ kind = .NONE };

// Feather predefined errors
ERR_STREAM_CONNECT :: Error.{ kind = .STREAM_CONNECT_FAILED, message = "Failed to connect to the stream" };
ERR_STREAM_READ    :: Error.{ kind = .STREAM_READ_FAILED, message = "Failed to read from the stream" };
ERR_STREAM_WRITE   :: Error.{ kind = .STREAM_WRITE_FAILED, message = "Failed to write to the stream" };
ERR_STREAM_ACCEPT  :: Error.{ kind = .STREAM_ACCEPT_FAILED, message = "Failed to accept connection" };
ERR_TCP_SOCKET     :: Error.{ kind = .TCP_SOCKET_CREATION_FAILED, message = "Failed to create TCP socket" };
ERR_TCP_NONBLOCK   :: Error.{ kind = .TCP_SET_NONBLOCK_FAILED, message = "Failed to set socket to non-blocking" };

// Falcon predefined errors
ERR_URL_PARSE      :: Error.{ kind = .URL_PARSE_FAILED, message = "Failed to parse URL" };
ERR_DNS_RESOLUTION :: Error.{ kind = .DNS_RESOLUTION_FAILED, message = "Failed to resolve DNS" };
ERR_MALFORMED_RESP :: Error.{ kind = .MALFORMED_SERVER_RESPONSE, message = "Malformed server response" };

// The 'guard' macro takes the error and bubbles it up.
// It is polymorphic ($T), so it accepts any struct that has a 'kind' field
// with a .NONE member!
guard :: (err: $T) #expand {
    if err.kind != .NONE {
        `return .{}, err;
    }
}
