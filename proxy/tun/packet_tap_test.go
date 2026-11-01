package tun

import (
	"bytes"
	"testing"
)

func TestPacketTapDisabledByDefault(t *testing.T) {
	DisablePacketTap()
	observePacket([]byte{1, 2, 3})
	if got := ReadPacketTap(); got != nil {
		t.Fatalf("expected no packet while disabled, got %v", got)
	}
}

func TestPacketTapCopiesPacket(t *testing.T) {
	if !EnablePacketTap(2) {
		t.Fatal("EnablePacketTap rejected valid capacity")
	}
	defer DisablePacketTap()

	source := []byte{1, 2, 3}
	observePacket(source)
	source[0] = 9

	got := ReadPacketTap()
	if !bytes.Equal(got, []byte{1, 2, 3}) {
		t.Fatalf("observer copy changed with source buffer: %v", got)
	}
}

func TestPacketTapCountsFullQueue(t *testing.T) {
	if !EnablePacketTap(1) {
		t.Fatal("EnablePacketTap rejected valid capacity")
	}
	defer DisablePacketTap()

	observePacket([]byte{1})
	observePacket([]byte{2})
	if got := PacketTapDropped(); got != 1 {
		t.Fatalf("expected one dropped observation, got %d", got)
	}
	if got := ReadPacketTap(); !bytes.Equal(got, []byte{1}) {
		t.Fatalf("unexpected queued packet: %v", got)
	}
}

func TestPacketTapPreservesQueuedCopiesAcrossDisableAndReenable(t *testing.T) {
	if !EnablePacketTap(2) {
		t.Fatal("EnablePacketTap rejected valid capacity")
	}
	observePacket([]byte{1, 2, 3})
	DisablePacketTap()
	if !EnablePacketTap(2) {
		t.Fatal("EnablePacketTap could not reuse queue after disable")
	}
	if got := ReadPacketTap(); !bytes.Equal(got, []byte{1, 2, 3}) {
		t.Fatalf("queued packet was lost across re-enable: %v", got)
	}
	DisablePacketTap()
}

func TestPacketTapRefusesResizeWhileQueuedCopiesRemain(t *testing.T) {
	if !EnablePacketTap(2) {
		t.Fatal("EnablePacketTap rejected valid capacity")
	}
	observePacket([]byte{4, 5})
	DisablePacketTap()
	if EnablePacketTap(3) {
		t.Fatal("resizing unexpectedly discarded a queued packet")
	}
	if got := ReadPacketTap(); !bytes.Equal(got, []byte{4, 5}) {
		t.Fatalf("queued packet was lost after rejected resize: %v", got)
	}
	if !EnablePacketTap(3) {
		t.Fatal("resize should succeed after old queue is drained")
	}
	DisablePacketTap()
}

func TestPacketTapRejectsInvalidCapacity(t *testing.T) {
	if EnablePacketTap(0) || EnablePacketTap(maxPacketTapCapacity+1) {
		t.Fatal("accepted invalid queue capacity")
	}
}

func TestPacketTapCopiesPacketPartsAndDrainsAfterDisable(t *testing.T) {
	if !EnablePacketTap(2) {
		t.Fatal("EnablePacketTap rejected valid capacity")
	}
	parts := [][]byte{{1, 2}, {3, 4, 5}}
	observePacketParts(parts, 5)
	DisablePacketTap()

	got := ReadPacketTap()
	if !bytes.Equal(got, []byte{1, 2, 3, 4, 5}) {
		t.Fatalf("unexpected packet assembled from readv parts: %v", got)
	}
	if got := ReadPacketTap(); got != nil {
		t.Fatalf("expected empty queue after drain, got %v", got)
	}
	if PacketTapDropped() != 0 {
		t.Fatalf("unexpected drops: %d", PacketTapDropped())
	}
}


func TestPacketTapResetRequiresDisabledQueue(t *testing.T) {
	if !EnablePacketTap(3) {
		t.Fatal("EnablePacketTap rejected valid capacity")
	}
	observePacket([]byte{1, 2, 3})
	if ResetPacketTap() {
		t.Fatal("reset unexpectedly succeeded while tap was enabled")
	}
	DisablePacketTap()
	if !ResetPacketTap() {
		t.Fatal("reset failed while tap was disabled")
	}
	if got := ReadPacketTap(); got != nil {
		t.Fatalf("reset left a stale packet queued: %v", got)
	}
	if got := PacketTapDropped(); got != 0 {
		t.Fatalf("reset did not clear drop counter: %d", got)
	}
	if got := PacketTapQueued(); got != 0 {
		t.Fatalf("reset did not clear queue depth: %d", got)
	}
	if got := PacketTapCapacity(); got != 3 {
		t.Fatalf("reset changed capacity: %d", got)
	}
	if PacketTapEnabled() {
		t.Fatal("reset unexpectedly enabled packet tap")
	}
}

func TestPacketTapResetStartsCleanSession(t *testing.T) {
	if !EnablePacketTap(2) {
		t.Fatal("EnablePacketTap rejected valid capacity")
	}
	observePacket([]byte{1})
	DisablePacketTap()
	if !ResetPacketTap() {
		t.Fatal("ResetPacketTap failed")
	}
	if !EnablePacketTap(2) {
		t.Fatal("EnablePacketTap could not start a clean session")
	}
	observePacket([]byte{9, 8})
	if got := ReadPacketTap(); !bytes.Equal(got, []byte{9, 8}) {
		t.Fatalf("new session returned stale or corrupt packet: %v", got)
	}
	if got := ReadPacketTap(); got != nil {
		t.Fatalf("new session retained old packet: %v", got)
	}
	DisablePacketTap()
}
