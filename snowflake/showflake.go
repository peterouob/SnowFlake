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

func (iw IdWorker) GetWorkerIdShift() int64 {
	return iw.workerIdShift
}

func (iw IdWorker) GetDatacenterIdShift() int64 {
	return iw.datacenterIdShift
}
func (iw IdWorker) GetDatacenterIdBits() int64 {
	return iw.datacenterIdBits
}

func (iw IdWorker) GetWorkerIdBits() int64 {
	return iw.workerIdBits
}

func (iw IdWorker) GetTimestampLeftShift() int64 {
	return iw.timestampLeftShift
}

func (iw IdWorker) GetTwepoch() int64 {
	return iw.twepoch
}

func (iw IdWorker) GetMachineId() uint64 {
	return (iw.NextId() >> iw.GetWorkerIdShift()) & ((1 << iw.GetDatacenterIdBits()) - 1)
}

func (iw IdWorker) GetDatacenterId() uint64 {
	return (iw.NextId() >> iw.GetDatacenterIdShift()) & ((1 << iw.GetDatacenterIdBits()) - 1)
}

func (iw IdWorker) GetTimeStamp() uint64 {
	return (iw.NextId() >> iw.GetTimestampLeftShift()) + uint64(iw.GetTwepoch())
}
