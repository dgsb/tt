package db

import (
	"fmt"
)

var (
	DuplicatedIntervalTagErr = fmt.Errorf("duplicated interval tags")
	ExistingOpenIntervalErr  = fmt.Errorf("already existing opened interval")
	IntervalTagsUnicityErr   = fmt.Errorf("interval_tags unicity failed")
	InvalidIntervalErr       = fmt.Errorf("invalid interval")
	InvalidStartTimestampErr = fmt.Errorf("invalid start timestamp")
	InvalidStopTimestampErr  = fmt.Errorf("invalid stop timestamp")
	MultipleOpenIntervalErr  = fmt.Errorf("multiple opened interval")
)
