package db

import (
	"fmt"
)

var (
	ErrDuplicatedIntervalTag = fmt.Errorf("duplicated interval tags")
	ErrExistingOpenInterval  = fmt.Errorf("already existing opened interval")
	ErrIntervalTagsUnicity   = fmt.Errorf("interval_tags unicity failed")
	ErrInvalidInterval       = fmt.Errorf("invalid interval")
	ErrInvalidParam          = fmt.Errorf("invalid parameter")
	ErrInvalidStartTimestamp = fmt.Errorf("invalid start timestamp")
	ErrInvalidStopTimestamp  = fmt.Errorf("invalid stop timestamp")
	ErrMultipleOpenInterval  = fmt.Errorf("multiple opened interval")
	ErrNotFound              = fmt.Errorf("not found entity")
	ErrNotImplemented        = fmt.Errorf("operation not implemented")
)
