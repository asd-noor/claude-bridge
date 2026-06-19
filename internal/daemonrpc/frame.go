// Package daemonrpc defines the length-prefixed JSON wire protocol shared by
// the daemon (server) and the shim (client) over a Unix domain socket. It is
// transport-only: the broker holds runtime state and lifecycle policy lives in
// the cmd layer, injected via hooks.
package daemonrpc

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
)

// MaxFrameSize caps a single frame's JSON payload to guard against oversized
// or malicious length prefixes.
const MaxFrameSize = 1 << 20 // 1 MiB

// frameHeaderSize is the width of the big-endian uint32 length prefix.
const frameHeaderSize = 4

// WriteFrame marshals v to JSON and writes it as a single length-prefixed
// frame: a big-endian uint32 length followed by the payload bytes.
func WriteFrame(w io.Writer, v any) error {
	payload, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("daemonrpc: marshal frame: %w", err)
	}
	if len(payload) > MaxFrameSize {
		return fmt.Errorf("daemonrpc: frame size %d exceeds max %d", len(payload), MaxFrameSize)
	}

	var header [frameHeaderSize]byte
	binary.BigEndian.PutUint32(header[:], uint32(len(payload)))
	if _, err := w.Write(header[:]); err != nil {
		return fmt.Errorf("daemonrpc: write frame header: %w", err)
	}
	if _, err := w.Write(payload); err != nil {
		return fmt.Errorf("daemonrpc: write frame payload: %w", err)
	}
	return nil
}

// ReadFrame reads one length-prefixed frame and returns its raw JSON payload.
// It rejects payloads larger than MaxFrameSize and surfaces truncated reads as
// io.ErrUnexpectedEOF.
func ReadFrame(r io.Reader) ([]byte, error) {
	var header [frameHeaderSize]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return nil, err
	}

	length := binary.BigEndian.Uint32(header[:])
	if length > MaxFrameSize {
		return nil, fmt.Errorf("daemonrpc: frame size %d exceeds max %d", length, MaxFrameSize)
	}

	payload := make([]byte, length)
	if _, err := io.ReadFull(r, payload); err != nil {
		return nil, err
	}
	return payload, nil
}
