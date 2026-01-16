package main

import "time"

type autoTuneResult struct {
	nextDML int32
	nextDDL int32
	fail    bool
}

func autoTuneStep(
	sinceAdvance time.Duration,
	successRate float64,
	activeDML int32,
	activeDDL int32,
	maxDML int32,
	maxDDL int32,
	soft time.Duration,
	hard time.Duration,
) autoTuneResult {
	if sinceAdvance >= hard {
		return autoTuneResult{fail: true}
	}

	nextDML := activeDML
	nextDDL := activeDDL

	if sinceAdvance >= soft || successRate < 0.10 {
		if nextDDL > 1 {
			nextDDL--
			return autoTuneResult{nextDML: nextDML, nextDDL: nextDDL}
		}
		if nextDML > 1 {
			nextDML -= 8
			if nextDML < 1 {
				nextDML = 1
			}
		}
		return autoTuneResult{nextDML: nextDML, nextDDL: nextDDL}
	}

	if nextDML < maxDML {
		nextDML += 8
		if nextDML > maxDML {
			nextDML = maxDML
		}
	}
	if nextDDL < maxDDL && sinceAdvance < soft/2 {
		nextDDL++
	}
	return autoTuneResult{nextDML: nextDML, nextDDL: nextDDL}
}
