package common

import "github.com/pingcap/ticdc/heartbeatpb"

type SyncPointControl struct {
	Epoch       uint64
	SkipStartTs uint64
	SkipEndTs   uint64
}

func NewDisabledSyncPointControl() SyncPointControl {
	return SyncPointControl{}
}

func NewSyncPointControlFromPB(control *heartbeatpb.SyncPointControl) SyncPointControl {
	if control == nil {
		return NewDisabledSyncPointControl()
	}
	return SyncPointControl{
		Epoch:       control.Epoch,
		SkipStartTs: control.SkipStartTs,
		SkipEndTs:   control.SkipEndTs,
	}
}

func (c SyncPointControl) ToPB() *heartbeatpb.SyncPointControl {
	return &heartbeatpb.SyncPointControl{
		Epoch:       c.Epoch,
		SkipStartTs: c.SkipStartTs,
		SkipEndTs:   c.SkipEndTs,
	}
}

func (c SyncPointControl) Disabled() bool {
	return c.SkipStartTs == 0 && c.SkipEndTs == 0
}

func (c SyncPointControl) IsOpenEnded() bool {
	return c.SkipStartTs != 0 && c.SkipEndTs == 0
}

func (c SyncPointControl) Contains(ts uint64) bool {
	if c.Disabled() || ts < c.SkipStartTs {
		return false
	}
	return c.SkipEndTs == 0 || ts < c.SkipEndTs
}

func (c SyncPointControl) Equal(other SyncPointControl) bool {
	return c.Epoch == other.Epoch &&
		c.SkipStartTs == other.SkipStartTs &&
		c.SkipEndTs == other.SkipEndTs
}

func (c SyncPointControl) Clone() SyncPointControl {
	return c
}
