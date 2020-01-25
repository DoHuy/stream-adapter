package main

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/veritone/edge-messages"
	"github.com/veritone/webstream-adapter/messaging"
)

type bytesReadTracker interface {
	BytesRead() int64
}

type bytesWrittenTracker interface {
	BytesWritten() int64
}

type heartbeat struct {
	sync.Mutex
	bytesReadTracker
	bytesWrittenTracker
	messageClient *messaging.Helper
	index         int64
	startTime     time.Time
	logOnlyMode   bool
}

func startHeartbeat(ctx context.Context, messageClient *messaging.Helper,
	heartbeatInterval time.Duration, bw bytesWrittenTracker, logOnlyMode bool) *heartbeat {

	hb := &heartbeat{
		bytesWrittenTracker: bw,
		messageClient:       messageClient,
		startTime:           time.Now(),
		logOnlyMode:         logOnlyMode,
	}

	// send the first heartbeat
	err := hb.sendHeartbeat(ctx, messages.EngineStatusRunning, nil)
	if err != nil {
		log.Println("WARNING: heartbeat message failed:", err)
	}

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-time.Tick(heartbeatInterval):
				err := hb.sendHeartbeat(ctx, messages.EngineStatusRunning, nil)
				if err != nil {
					log.Println("WARNING: heartbeat message failed:", err)
				}
			}
		}
	}()

	return hb
}

func (h heartbeat) GetTaskUpTime() int64 {
	return int64(time.Now().Sub(h.startTime) / time.Millisecond)
}

func (h *heartbeat) trackReads(br bytesReadTracker) {
	h.Lock()
	defer h.Unlock()
	h.bytesReadTracker = br
}

func (h *heartbeat) sendHeartbeat(ctx context.Context, status messages.EngineStatus, err error) error {
	h.Lock()
	defer h.Unlock()
	h.index++

	if h.logOnlyMode {
		bs := datasize.ByteSize(h.BytesWritten()) * datasize.B
		log.Printf("-- written: %s, status: %s", bs.HR(), status)
		return nil
	}

	msg := messages.EmptyEngineHeartbeat()
	msg.Count = h.index
	msg.Status = status
	msg.UpTime = h.GetTaskUpTime()

	if h.bytesReadTracker != nil {
		msg.BytesRead = h.BytesRead()
	}
	if h.bytesWrittenTracker != nil {
		msg.BytesWritten = h.BytesWritten()
	}
	if err != nil {
		errReason, ok := err.(errorReason)
		if ok && errReason.error != nil {
			msg.ErrorMsg = errReason.Error()
			msg.FailureMsg = errReason.Error()
			msg.FailureReason = errReason.failureReason
		} else {
			msg.ErrorMsg = err.Error()
			msg.FailureMsg = err.Error()
			msg.FailureReason = messages.FailureReasonOther
		}
	}
	return h.messageClient.ProduceHeartbeatMessage(ctx, msg)
}
