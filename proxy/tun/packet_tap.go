package tun

import (
	"sync/atomic"
)

const (
	minPacketTapCapacity = 1
	maxPacketTapCapacity = 1024
)

type packetTap struct {
	packets chan []byte
	dropped atomic.Uint64
}

var activePacketTap atomic.Pointer[packetTap]

// EnablePacketTap enables a bounded, non-blocking copy of packets read from the TUN.
// It returns false when capacity is outside the supported range.
func EnablePacketTap(capacity int) bool {
	if capacity < minPacketTapCapacity || capacity > maxPacketTapCapacity {
		return false
	}
	activePacketTap.Store(&packetTap{packets: make(chan []byte, capacity)})
	return true
}

// DisablePacketTap stops observing new packets. Packets already queued are discarded.
func DisablePacketTap() {
	activePacketTap.Store(nil)
}

// ReadPacketTap returns one copied packet, or nil when the queue is empty.
func ReadPacketTap() []byte {
	tap := activePacketTap.Load()
	if tap == nil {
		return nil
	}
	select {
	case packet := <-tap.packets:
		return packet
	default:
		return nil
	}
}

// PacketTapDropped returns the number of observation copies dropped because the
// bounded queue was full since the most recent EnablePacketTap call.
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

	// ReadPacket reuses its input buffer, so the observer must own its copy.
	copyOfPacket := append([]byte(nil), packet...)
	select {
	case tap.packets <- copyOfPacket:
	default:
		tap.dropped.Add(1)
	}
}
