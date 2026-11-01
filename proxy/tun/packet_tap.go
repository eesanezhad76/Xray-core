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
var packetTapConfigMu sync.Mutex

// EnablePacketTap enables a bounded packet copy queue. Packet buffers are sized
// to the actual packet length rather than reserving 65535 bytes for every slot.
func EnablePacketTap(capacity int) bool {
	if capacity < minPacketTapCapacity || capacity > maxPacketTapCapacity {
		return false
	}
	packetTapConfigMu.Lock()
	defer packetTapConfigMu.Unlock()

	if current := activePacketTap.Load(); current != nil {
		current.mu.Lock()
		if len(current.slots) == capacity {
			// Reuse the existing queue across a core reload. Replacing it would
			// make packets already copied into the old queue unreachable.
			current.enabled.Store(true)
			current.mu.Unlock()
			return true
		}
		if current.queued != 0 {
			// Never discard queued observations just to resize a diagnostic queue.
			current.mu.Unlock()
			return false
		}
		current.enabled.Store(false)
		current.mu.Unlock()
	}

	tap := &packetTap{slots: make([]packetTapSlot, capacity)}
	tap.enabled.Store(true)
	activePacketTap.Store(tap)
	return true
}

// DisablePacketTap stops observing new packets but leaves queued packets readable.
func DisablePacketTap() {
	packetTapConfigMu.Lock()
	defer packetTapConfigMu.Unlock()
	if tap := activePacketTap.Load(); tap != nil {
		tap.enabled.Store(false)
	}
}


// ResetPacketTap clears queued observation copies and the drop counter.
// It is intentionally allowed only while observation is disabled. Callers must
// stop and join the old consumer before resetting, then enable a fresh session.
func ResetPacketTap() bool {
	packetTapConfigMu.Lock()
	defer packetTapConfigMu.Unlock()

	tap := activePacketTap.Load()
	if tap == nil {
		return true
	}
	tap.mu.Lock()
	defer tap.mu.Unlock()
	if tap.enabled.Load() {
		return false
	}
	for i := range tap.slots {
		tap.slots[i].data = nil
	}
	tap.head = 0
	tap.tail = 0
	tap.queued = 0
	tap.dropped.Store(0)
	return true
}

// PacketTapQueued returns the number of copied packets waiting for the consumer.
func PacketTapQueued() int {
	tap := activePacketTap.Load()
	if tap == nil {
		return 0
	}
	tap.mu.Lock()
	defer tap.mu.Unlock()
	return tap.queued
}

// PacketTapCapacity returns the configured queue capacity, or zero before first enable.
func PacketTapCapacity() int {
	tap := activePacketTap.Load()
	if tap == nil {
		return 0
	}
	return len(tap.slots)
}

// PacketTapEnabled reports whether new observation copies are currently accepted.
func PacketTapEnabled() bool {
	tap := activePacketTap.Load()
	return tap != nil && tap.enabled.Load()
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

	// Packet observation must never stall the TUN forwarding path. If the
	// reader briefly owns the queue lock, drop this diagnostic copy and count it.
	if !tap.mu.TryLock() {
		tap.dropped.Add(1)
		return
	}
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
