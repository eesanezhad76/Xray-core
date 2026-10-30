package tun

import (
	"sync"
	"sync/atomic"
)

const (
	minPacketTapCapacity = 1
	maxPacketTapCapacity = 128
	maxPacketTapSize     = 65535
)

type packetTapSlot struct {
	data []byte
	size int
}

type packetTap struct {
	mu      sync.Mutex
	slots   []packetTapSlot
	head    int
	tail    int
	queued  int
	dropped atomic.Uint64
}

var activePacketTap atomic.Pointer[packetTap]

// EnablePacketTap enables a bounded, preallocated, non-blocking copy of packets
// read from the TUN. Capacity must be between 1 and 128 packets.
func EnablePacketTap(capacity int) bool {
	if capacity < minPacketTapCapacity || capacity > maxPacketTapCapacity {
		return false
	}
	tap := &packetTap{slots: make([]packetTapSlot, capacity)}
	for i := range tap.slots {
		tap.slots[i].data = make([]byte, maxPacketTapSize)
	}
	activePacketTap.Store(tap)
	return true
}

// DisablePacketTap stops observing new packets. Packets already queued are discarded.
func DisablePacketTap() {
	activePacketTap.Store(nil)
}

// ReadPacketTap returns one copied packet, or nil when the queue is empty or busy.
func ReadPacketTap() []byte {
	tap := activePacketTap.Load()
	if tap == nil || !tap.mu.TryLock() {
		return nil
	}
	if tap.queued == 0 {
		tap.mu.Unlock()
		return nil
	}

	slot := &tap.slots[tap.head]
	packet := append([]byte(nil), slot.data[:slot.size]...)
	slot.size = 0
	tap.head = (tap.head + 1) % len(tap.slots)
	tap.queued--
	tap.mu.Unlock()
	return packet
}

// PacketTapDropped returns the number of observation copies dropped because
// the queue was full, the packet was oversized, or the queue was busy.
func PacketTapDropped() uint64 {
	tap := activePacketTap.Load()
	if tap == nil {
		return 0
	}
	return tap.dropped.Load()
}

func observePacket(packet []byte) {
	tap := activePacketTap.Load()
	if tap == nil || len(packet) == 0 {
		return
	}
	if len(packet) > maxPacketTapSize || !tap.mu.TryLock() {
		tap.dropped.Add(1)
		return
	}
	if tap.queued == len(tap.slots) {
		tap.mu.Unlock()
		tap.dropped.Add(1)
		return
	}

	slot := &tap.slots[tap.tail]
	copy(slot.data, packet)
	slot.size = len(packet)
	tap.tail = (tap.tail + 1) % len(tap.slots)
	tap.queued++
	tap.mu.Unlock()
}
