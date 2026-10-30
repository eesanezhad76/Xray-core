package tun

import (
	"sync"
	"sync/atomic"
)

const (
	minPacketTapCapacity = 1
	maxPacketTapCapacity = 4096
	maxPacketTapSize     = 65535
)

type packetTapSlot struct {
	data []byte
}

type packetTap struct {
	mu      sync.Mutex
	slots   []packetTapSlot
	head    int
	tail    int
	queued  int
	enabled atomic.Bool
	dropped atomic.Uint64
}

var activePacketTap atomic.Pointer[packetTap]

// EnablePacketTap enables a bounded packet copy queue. Packet buffers are sized
// to the actual packet length rather than reserving 65535 bytes for every slot.
func EnablePacketTap(capacity int) bool {
	if capacity < minPacketTapCapacity || capacity > maxPacketTapCapacity {
		return false
	}
	tap := &packetTap{slots: make([]packetTapSlot, capacity)}
	tap.enabled.Store(true)
	activePacketTap.Store(tap)
	return true
}

// DisablePacketTap stops observing new packets but leaves queued packets readable.
func DisablePacketTap() {
	if tap := activePacketTap.Load(); tap != nil {
		tap.enabled.Store(false)
	}
}

// ReadPacketTap returns the next owned raw IP packet, or nil when the queue is empty.
func ReadPacketTap() []byte {
	tap := activePacketTap.Load()
	if tap == nil {
		return nil
	}
	tap.mu.Lock()
	if tap.queued == 0 {
		tap.mu.Unlock()
		return nil
	}
	slot := &tap.slots[tap.head]
	packet := slot.data
	slot.data = nil
	tap.head = (tap.head + 1) % len(tap.slots)
	tap.queued--
	tap.mu.Unlock()
	return packet
}

// PacketTapDropped counts packets that could not be queued because the queue was full
// or a packet exceeded the maximum supported IP packet size.
func PacketTapDropped() uint64 {
	tap := activePacketTap.Load()
	if tap == nil {
		return 0
	}
	return tap.dropped.Load()
}

func observePacket(packet []byte) {
	observePacketParts([][]byte{packet}, len(packet))
}

// observePacketParts copies one packet spread across the TUN readv buffers.
func observePacketParts(parts [][]byte, size int) {
	tap := activePacketTap.Load()
	if tap == nil || !tap.enabled.Load() || size == 0 {
		return
	}
	if size < 0 || size > maxPacketTapSize {
		tap.dropped.Add(1)
		return
	}

	// Blocking lock prevents transient reader contention from silently dropping
	// packets; the critical section only copies/enqueues the packet.
	tap.mu.Lock()
	defer tap.mu.Unlock()
	if !tap.enabled.Load() {
		return
	}
	if tap.queued == len(tap.slots) {
		tap.dropped.Add(1)
		return
	}

	packet := make([]byte, size)
	offset := 0
	for _, part := range parts {
		if offset == size {
			break
		}
		offset += copy(packet[offset:], part[:min(len(part), size-offset)])
	}
	if offset != size {
		tap.dropped.Add(1)
		return
	}
	tap.slots[tap.tail].data = packet
	tap.tail = (tap.tail + 1) % len(tap.slots)
	tap.queued++
}
