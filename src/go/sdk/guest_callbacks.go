package redpanda

import (
	"time"
	"unsafe"

	"github.com/rockwotj/redpanda/src/go/sdk/internal/rwbuf"
)

type eventErrorCode int32

const (
	evtSuccess       = eventErrorCode(0)
	evtConfigError   = eventErrorCode(1)
	evtUserError     = eventErrorCode(2)
	evtInternalError = eventErrorCode(3)
)

type inputBatchHandle int32
type inputRecordHandle int32

type batchHeader struct {
	handle               inputBatchHandle
	baseOffset           int64
	recordCount          int
	partitionLeaderEpoch int
	attributes           int16
	lastOffsetDelta      int
	baseTimestamp        int64
	maxTimestamp         int64
	producerId           int64
	producerEpoch        int16
	baseSequence         int
}

// Cache a bunch of objects to not GC
var (
	currentHeader batchHeader  = batchHeader{handle: -1}
	inbuf         *rwbuf.RWBuf = rwbuf.New(2048)
	outbuf        *rwbuf.RWBuf = rwbuf.New(2048)
	e             writeEvent
)

// The ABI that our SDK provides. Redpanda executes this function to determine the protocol contract to execute.
//export redpanda_abi_version
func redpandaAbiVersion() int32 {
	return 2
}

//export redpanda_on_record
func redpandaOnRecord(bh inputBatchHandle, rh inputRecordHandle, recordSize int, currentRelativeOutputOffset int) eventErrorCode {
	if userTransformFunction == nil {
		println("Invalid configuration, there is no registered user transform function")
		return evtConfigError
	}
	if currentHeader.handle != bh {
		currentHeader.handle = bh
		errno := readRecordHeader(
			bh,
			unsafe.Pointer(&currentHeader.baseOffset),
			unsafe.Pointer(&currentHeader.recordCount),
			unsafe.Pointer(&currentHeader.partitionLeaderEpoch),
			unsafe.Pointer(&currentHeader.attributes),
			unsafe.Pointer(&currentHeader.lastOffsetDelta),
			unsafe.Pointer(&currentHeader.baseTimestamp),
			unsafe.Pointer(&currentHeader.maxTimestamp),
			unsafe.Pointer(&currentHeader.producerId),
			unsafe.Pointer(&currentHeader.producerEpoch),
			unsafe.Pointer(&currentHeader.baseSequence),
		)
		if errno != 0 {
			println("Failed to read batch header")
			return evtInternalError
		}
	}
	inbuf.Reset()
	inbuf.EnsureSize(recordSize)
	amt := int(readRecord(rh, unsafe.Pointer(inbuf.WriterBufPtr()), int32(recordSize)))
	inbuf.AdvanceWriter(amt)
	if amt != recordSize {
		println("reading record failed with errno:", amt)
		return evtInternalError
	}
	err := e.record.deserialize(inbuf)
	if err != nil {
		println("deserializing record failed:", err.Error())
		return evtInternalError
	}
	// Save the original timestamp for output records
	ot := e.Record().Timestamp
	// Fix up the offsets to be absolute values
	e.record.Offset += currentHeader.baseOffset
	if e.record.Attrs.TimestampType() == 0 {
		e.record.Timestamp = time.UnixMilli(e.record.Timestamp.UnixMilli() + currentHeader.baseTimestamp)
	} else {
		e.record.Timestamp = time.UnixMilli(currentHeader.maxTimestamp)
	}
	rs, err := userTransformFunction(&e)
	if err != nil {
		println("transforming record failed:", err.Error())
		return evtUserError
	}
	if rs == nil {
		return evtSuccess
	}
	for i, r := range rs {
		// Because the previous record in the batch could have
		// output multiple records, we need to account for this,
		// by adjusting the offset accordingly.
		r.Offset = int64(currentRelativeOutputOffset + i)
		// Keep the same timestamp as the input record.
		r.Timestamp = ot

		outbuf.Reset()
		r.serialize(outbuf)
		b := outbuf.ReadAll()
		amt := int(writeRecord(unsafe.Pointer(&b[0]), int32(len(b))))
		if amt != len(b) {
			println("writing record failed with errno:", amt)
			return evtInternalError
		}
	}
	return evtSuccess
}
