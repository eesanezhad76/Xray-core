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

func TestPacketTapDropsWhenFullWithoutBlocking(t *testing.T) {
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

func TestPacketTapRejectsInvalidCapacity(t *testing.T) {
	if EnablePacketTap(0) || EnablePacketTap(maxPacketTapCapacity+1) {
		t.Fatal("accepted invalid queue capacity")
	}
}
