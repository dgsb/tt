package main

import (
	"fmt"
	"io"
	"sort"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/dgsb/tt/internal/db"
)

func sameDate(t1, t2 time.Time) bool {
	year1, month1, day1 := t1.UTC().Date()
	year2, month2, day2 := t2.UTC().Date()
	return year1 == year2 && month1 == month2 && day1 == day2
}

func FlatReport(tas []db.TaggedInterval, out io.Writer) error {
	if !sort.SliceIsSorted(tas, func(i, j int) bool {
		return tas[i].Interval.StartTimestamp.Unix() < tas[j].Interval.StartTimestamp.Unix()
	}) {
		return fmt.Errorf("input tagged interval is not sorted")
	}

	tab := tabwriter.NewWriter(out, 16, 4, 0, ' ', 0)

	var prevStartTime time.Time
	for _, ta := range tas {
		if !sameDate(prevStartTime, ta.Interval.StartTimestamp) {
			tab.Write([]byte(ta.Interval.StartTimestamp.Format("2006-01-02")))
		}
		tab.Write([]byte("\t"))
		tab.Write([]byte(ta.Interval.ID))
		tab.Write([]byte("\t"))
		tab.Write([]byte(ta.Interval.StartTimestamp.Format("15:04:05")))
		tab.Write([]byte("\t"))
		tab.Write([]byte(ta.Interval.StopTimestamp.Format("15:04:05")))
		tab.Write([]byte("\t"))

		if ta.Interval.StopTimestamp.IsZero() {
			ta.Interval.StopTimestamp = time.Now().Truncate(time.Second)
		}
		tab.Write([]byte(ta.Interval.StopTimestamp.Sub(ta.Interval.StartTimestamp).String()))
		tab.Write([]byte("\t"))

		tab.Write([]byte(strings.Join(ta.Tags, ",")))
		tab.Write([]byte("\t"))

		tab.Write([]byte("\n"))

		prevStartTime = ta.Interval.StartTimestamp
	}
	tab.Flush()

	return nil
}
