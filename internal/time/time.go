// Package time provides an extended time type which allows
// to unmarshal from a text string discovering automatically its format.
package time

import (
	"fmt"
	"time"
)

var UnparsableTimesampFormatErr = fmt.Errorf("unparsable timestamp format")
var (
	local = time.Local
	now   = time.Now
)

type Time time.Time

func (t *Time) UnmarshalText(data []byte) error {

	var (
		otherT time.Time
		err    error
	)

	if otherT, err = time.Parse(time.RFC3339, string(data)); err == nil {
		*t = Time(otherT)
		return nil
	}

	// Use the local time zone when not specified
	if otherT, err = time.ParseInLocation("2006-01-02T15:04:05", string(data), time.Local); err == nil {
		*t = Time(otherT)
		return nil
	}

	// Use the current day in local timezone when only the time part is specified
	if otherT, err = time.ParseInLocation("15:04", string(data), time.Local); err == nil {
		year, month, day := now().Local().Date()
		*t = Time(time.Date(year, month, day, otherT.Hour(), otherT.Minute(), otherT.Second(), 0, local))
		return nil
	}

	return fmt.Errorf("%w: %s", UnparsableTimesampFormatErr, string(data))
}

func (t *Time) Time() time.Time {
	return time.Time(*t)
}
