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
		return fmt.Errorf("%w: input tagged interval is not sorted", errInvalidParameter)
	}

	tab := tabwriter.NewWriter(out, 16, 4, 0, ' ', 0)

	var prevStartTime time.Time
	var totalDuration time.Duration
	var err error
	twrite := func(s string) {
		if err != nil {
			return
		}
		_, err = tab.Write([]byte(s))
	}
	for i := 0; i < len(tas) && err == nil; i++ {
		ta := tas[i]
		if !sameDate(prevStartTime, ta.Interval.StartTimestamp) {
			twrite(ta.Interval.StartTimestamp.Format("2006-01-02"))
		}
		twrite("\t")
		twrite(ta.Interval.ID)
		twrite("\t")
		twrite(ta.Interval.StartTimestamp.Format("15:04:05"))
		twrite("\t")
		twrite(ta.Interval.StopTimestamp.Format("15:04:05"))
		twrite("\t")

		if ta.Interval.StopTimestamp.IsZero() {
			ta.Interval.StopTimestamp = time.Now().Truncate(time.Second)
		}
		duration := ta.Interval.StopTimestamp.Sub(ta.Interval.StartTimestamp)
		totalDuration += duration
		twrite(duration.String())
		twrite("\t")

		twrite(strings.Join(ta.Tags, ","))
		twrite("\t")

		twrite("\n")

		prevStartTime = ta.Interval.StartTimestamp
	}
	twrite("\n")
	twrite("Total time")
	twrite("\t\t\t\t")
	twrite(totalDuration.String())
	twrite("\n")
	if err == nil {
		err = tab.Flush()
	}

	return err
}
