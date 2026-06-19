package daemonrpc

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"testing"
)

func TestWriteReadFrameRoundtrip(t *testing.T) {
	want := Request{Method: MethodSend, SessionID: "sess-1", Params: json.RawMessage(`{"to":"x"}`)}

	var buf bytes.Buffer
	if err := WriteFrame(&buf, want); err != nil {
		t.Fatalf("WriteFrame: %v", err)
	}

	raw, err := ReadFrame(&buf)
	if err != nil {
		t.Fatalf("ReadFrame: %v", err)
	}

	var got Request
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.Method != want.Method || got.SessionID != want.SessionID {
		t.Fatalf("roundtrip mismatch: got %+v want %+v", got, want)
	}
}

func TestReadFrameRejectsOversize(t *testing.T) {
	var header [frameHeaderSize]byte
	binary.BigEndian.PutUint32(header[:], MaxFrameSize+1)

	_, err := ReadFrame(bytes.NewReader(header[:]))
	if err == nil {
		t.Fatal("expected oversize rejection, got nil")
	}
}

func TestWriteFrameRejectsOversize(t *testing.T) {
	oversize := make([]byte, MaxFrameSize+1)
	for i := range oversize {
		oversize[i] = 'a'
	}

	var buf bytes.Buffer
	if err := WriteFrame(&buf, string(oversize)); err == nil {
		t.Fatal("expected oversize write rejection, got nil")
	}
}

func TestReadFrameTruncatedHeader(t *testing.T) {
	_, err := ReadFrame(bytes.NewReader([]byte{0x00, 0x01}))
	if err == nil {
		t.Fatal("expected error on truncated header")
	}
}

func TestReadFrameTruncatedPayload(t *testing.T) {
	var header [frameHeaderSize]byte
	binary.BigEndian.PutUint32(header[:], 10) // claim 10 bytes, supply 3

	r := io.MultiReader(bytes.NewReader(header[:]), bytes.NewReader([]byte("abc")))
	_, err := ReadFrame(r)
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("expected io.ErrUnexpectedEOF, got %v", err)
	}
}

func TestMultipleFramesBackToBack(t *testing.T) {
	var buf bytes.Buffer
	frames := []Request{
		{Method: MethodRegister},
		{Method: MethodListPeers, SessionID: "s2"},
		{Method: MethodPoll, SessionID: "s3"},
	}
	for _, f := range frames {
		if err := WriteFrame(&buf, f); err != nil {
			t.Fatalf("WriteFrame: %v", err)
		}
	}

	for i, want := range frames {
		raw, err := ReadFrame(&buf)
		if err != nil {
			t.Fatalf("ReadFrame %d: %v", i, err)
		}
		var got Request
		if err := json.Unmarshal(raw, &got); err != nil {
			t.Fatalf("unmarshal %d: %v", i, err)
		}
		if got.Method != want.Method || got.SessionID != want.SessionID {
			t.Fatalf("frame %d mismatch: got %+v want %+v", i, got, want)
		}
	}

	if _, err := ReadFrame(&buf); !errors.Is(err, io.EOF) {
		t.Fatalf("expected io.EOF after last frame, got %v", err)
	}
}
