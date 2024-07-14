package snowflake

import (
	"log"
	"time"
)

type IdWorkerInterface interface {
	NextId() uint64
	GetWorkerIdShift() int64
	GetDatacenterIdShift() int64
	GetDatacenterIdBits() int64
	GetWorkerIdBits() int64
	GetTimestampLeftShift() int64
	GetTwepoch() int64
	GetMachineId() uint64
	GetDatacenterId() uint64
	GetTimeStamp() uint64
}

type IdWorker struct {
	workId       uint64
	datacenterId uint64
	sequence     int64

	twepoch          int64
	workerIdBits     int64
	datacenterIdBits int64
	maxWorkerid      int64
	maxDatacenterId  int64
	sequenceBits     int64

	workerIdShift      int64
	datacenterIdShift  int64
	timestampLeftShift int64
	sequenceMask       int64
	lastTimestamp      int64
}

var _ IdWorkerInterface = (*IdWorker)(nil)

func NewIdWorker(workId, datacenterId uint64, sequence, workerIdBits, datacenterIdBits, sequenceBits int64) *IdWorker {
	return &IdWorker{
		workId:             workId,
		datacenterId:       datacenterId,
		sequence:           sequence,
		twepoch:            1288834974657,
		workerIdBits:       workerIdBits,
		datacenterIdBits:   datacenterIdBits,
		maxWorkerid:        -1 ^ (-1 << workerIdBits),
		maxDatacenterId:    -1 ^ (-1 << datacenterIdBits),
		sequenceBits:       sequenceBits,
		workerIdShift:      sequenceBits,
		datacenterIdShift:  sequence + workerIdBits,
		timestampLeftShift: sequenceBits + workerIdBits + datacenterIdBits,
		sequenceMask:       -1 ^ (-1 << sequenceBits),
		lastTimestamp:      -1,
	}
}

func (iw *IdWorker) NextId() uint64 {
	var timestamp int64 = timeGen()
	if timestamp < iw.lastTimestamp {
		log.Printf("Clock moved backwards. Waiting for %d milliseconds", iw.lastTimestamp-timestamp)
		time.Sleep(time.Duration(iw.lastTimestamp-timestamp) * time.Millisecond)
	}
	if iw.lastTimestamp == timestamp {
		iw.sequence = (iw.sequence + 1) & iw.sequenceMask
		if iw.sequence == 0 {
			timestamp = tilNextMillis(iw.lastTimestamp)
		}
	} else {
		iw.sequence = 0
	}
	iw.lastTimestamp = timestamp
	return uint64((timestamp-iw.twepoch)<<iw.timestampLeftShift |
		(iw.datacenterIdBits << iw.datacenterIdShift) |
		(iw.workerIdBits << iw.workerIdShift) |
		iw.sequence)
}

func tilNextMillis(lastTimestamp int64) int64 {
	var timestamp int64 = timeGen()
	for timestamp <= lastTimestamp {
		timestamp = timeGen()
	}
	return timestamp
}

func timeGen() int64 {
	return time.Now().UnixMilli()
}

func (i IdWorker) GetWorkerIdShift() int64 {
	return i.workerIdShift
}

func (i IdWorker) GetDatacenterIdShift() int64 {
	return i.datacenterIdShift
}
func (i IdWorker) GetDatacenterIdBits() int64 {
	return i.datacenterIdBits
}

func (i IdWorker) GetWorkerIdBits() int64 {
	return i.workerIdBits
}

func (i IdWorker) GetTimestampLeftShift() int64 {
	return i.timestampLeftShift
}

func (i IdWorker) GetTwepoch() int64 {
	return i.twepoch
}

func (i IdWorker) GetMachineId() uint64 {
	return (i.NextId() >> i.GetWorkerIdShift()) & ((1 << i.GetDatacenterIdBits()) - 1)
}

func (i IdWorker) GetDatacenterId() uint64 {
	return (i.NextId() >> i.GetDatacenterIdShift()) & ((1 << i.GetDatacenterIdBits()) - 1)
}

func (i IdWorker) GetTimeStamp() uint64 {
	return (i.NextId() >> i.GetTimestampLeftShift()) + uint64(i.GetTwepoch())
}
