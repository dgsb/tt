package main

import (
	"database/sql"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/alecthomas/kong"
	"github.com/sirupsen/logrus"

	"github.com/dgsb/tt/internal/db"
	itime "github.com/dgsb/tt/internal/time"
)

type CommonConfig struct {
	Database string `name:"db" type:"file" default:"${home}/.tt.db" help:"the sqlite database to use for application data"`
}

type StartCmd struct {
	At   itime.Time    `help:"specify the start timestamp in RFC3339 format" group:"time" xor:"time"`
	Ago  time.Duration `help:"specify the start timestamp as a duration in the past" group:"time" xor:"time"`
	Tags []string      `arg:"" optional:"" help:"the value to tag the interval with"`
}

func (cmd *StartCmd) Run(tt *db.TimeTracker) error {
	startTime := time.Now()
	if !cmd.At.Time().IsZero() {
		startTime = cmd.At.Time()
	} else if cmd.Ago != 0 {
		startTime = time.Now().Add(-cmd.Ago)
	}

	// Stop the current interval before opening a new one
	if err := tt.Stop(startTime); err != nil && !errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("cannot stop currently opened interval: %w", err)
	}

	if err := tt.Start(startTime, cmd.Tags); err != nil {
		return fmt.Errorf("cannot start a new opened interval: %w", err)
	}

	return nil
}

type StopCmd struct {
	At  itime.Time    `help:"specify the stop timestamp in RFC3339 format" group:"time" xor:"time"`
	Ago time.Duration `help:"specify the stop timestamp as a duration in the past" group:"time" xor:"time"`
}

func (cmd *StopCmd) Run(tt *db.TimeTracker) error {
	stopTime := time.Now()
	if !cmd.At.Time().IsZero() {
		stopTime = cmd.At.Time()
	} else if cmd.Ago != 0 {
		stopTime = time.Now().Add(-cmd.Ago)
	}

	if err := tt.Stop(stopTime); err != nil {
		return fmt.Errorf("cannot stop a currently opened interval: %w", err)
	}

	return nil
}

type ListCmd struct {
	At     itime.Time `help:"another starting point for the required time period instead of now"`
	Period string     `arg:"" help:"a logical description of the time period to look at" default:":day" enum:":week,:day,:month,:year"`
}

func (cmd *ListCmd) Run(tt *db.TimeTracker) error {
	startTime := cmd.At.Time()
	if startTime.IsZero() {
		startTime = time.Now()
	}

	var stopTime time.Time
	switch cmd.Period {
	case ":day":
		year, month, day := startTime.Date()
		startTime = time.Date(year, month, day, 0, 0, 0, 0, time.Local)
		stopTime = time.Date(year, month, day+1, 0, 0, 0, 0, time.Local)
	case ":week":
		year, month, day := startTime.Date()
		weekday := startTime.Weekday()
		if weekday == time.Sunday {
			weekday = time.Saturday + 1
		}
		startTime = time.Date(year, month, day-int(weekday-time.Monday), 0, 0, 0, 0, time.Local)
		stopTime = time.Date(year, month, day+1+int(time.Saturday+1-weekday), 0, 0, 0, 0, time.Local)
	case ":month":
		year, month, _ := startTime.Date()
		startTime = time.Date(year, month, 1, 0, 0, 0, 0, time.Local)
		stopTime = time.Date(year, month+1, 1, 0, 0, 0, 0, time.Local)
	case ":year":
		year, _, _ := startTime.Date()
		startTime = time.Date(year, time.January, 1, 0, 0, 0, 0, time.Local)
		stopTime = time.Date(year+1, time.January, 1, 0, 0, 0, 0, time.Local)
	default:
		return fmt.Errorf("this period is not yet implemented: %s", cmd.Period)
	}

	taggedIntervals, err := tt.List(startTime, stopTime)
	if err != nil {
		return fmt.Errorf("cannot list recorded interval: %w", err)
	}

	return FlatReport(taggedIntervals, os.Stdout)
}

type DeleteCmd struct {
	IDs []string `arg:"" name:"ids" help:"the ids of the intervals to delete"`
}

func (cmd *DeleteCmd) Run(tt *db.TimeTracker) error {
	for _, id := range cmd.IDs {
		if err := tt.Delete(id); err != nil {
			return fmt.Errorf("cannot delete interval %s: %w", id, err)
		}
	}

	return nil
}

type TagCmd struct {
	ID   string   `arg:"" help:"the interval id to tag"`
	Tags []string `arg:"" help:"values to tag the interval with"`
}

func (cmd *TagCmd) Run(tt *db.TimeTracker) error {
	if err := tt.Tag(cmd.ID, cmd.Tags); err != nil {
		return fmt.Errorf("cannot tag interval %s with %s: %w", cmd.ID, cmd.Tags, err)
	}

	return nil
}

type UntagCmd struct {
	ID   string   `arg:"" help:"the interval id to untag"`
	Tags []string `arg:"" help:"the tag to remove from the interval"`
}

func (cmd *UntagCmd) Run(tt *db.TimeTracker) error {
	if err := tt.Untag(cmd.ID, cmd.Tags); err != nil {
		return fmt.Errorf("cannot untag %s from %s: %w", cmd.ID, cmd.Tags, err)
	}
	return nil
}

type CurrentCmd struct {
}

func (cmd *CurrentCmd) Run(tt *db.TimeTracker) error {
	interval, err := tt.Current()
	if err != nil {
		return fmt.Errorf("cannot retrieve current interval: %w", err)
	}
	if interval != nil {
		return FlatReport([]db.TaggedInterval{*interval}, os.Stdout)
	}
	return nil
}

type ContinueCmd struct {
	ID string `long:"id" help:"specify an interval ID to continue"`
}

func (cmd *ContinueCmd) Run(tt *db.TimeTracker) error {
	if err := tt.Continue(time.Now(), cmd.ID); err != nil {
		return fmt.Errorf("cannot continue a previously closed interval: %w", err)
	}

	return nil
}

type VacuumCmd struct {
	Since  time.Duration `required:"" help:"specify the duration to delete data before" group:"time" xor:"time"`
	Before time.Time     `required:"" help:"specify the timestamp to delete data before" group:"time" xor:"time"`
}

func (cmd *VacuumCmd) Run(tt *db.TimeTracker) error {
	checkpoint := cmd.Before
	if checkpoint.IsZero() {
		checkpoint = time.Now().Add(-cmd.Since)
	}

	if err := tt.Vacuum(checkpoint); err != nil {
		return fmt.Errorf("cannot vacuum the database: %w", err)
	}

	return nil
}

func main() {

	homeDir, err := os.UserHomeDir()
	if err != nil {
		logrus.WithError(err).Fatal("cannot retrieve user home directory")
	}

	var CLI struct {
		CommonConfig

		Continue ContinueCmd `cmd:"" help:"start a new interval with same tags as the last closed one"`
		Current  CurrentCmd  `default:"1" cmd:"" help:"return the current opened interval"`
		Delete   DeleteCmd   `cmd:"" help:"delete a registered interval"`
		List     ListCmd     `cmd:"" help:"list intervals"`
		Start    StartCmd    `cmd:"" help:"start tracking a new time interval"`
		Stop     StopCmd     `cmd:"" help:"stop tracking the current opened interval"`
		Tag      TagCmd      `cmd:"" help:"tag an interval with given values"`
		Untag    UntagCmd    `cmd:"" help:"remove tags from an interval"`
		Vacuum   VacuumCmd   `cmd:"" help:"hard delete old soft deleted data"`
	}

	ctx := kong.Parse(&CLI, kong.Vars{"home": homeDir})

	tt, err := db.New(CLI.CommonConfig.Database)
	if err != nil {
		logrus.WithError(err).Fatal("cannot setup application database")
	}
	defer tt.Close()

	if err := ctx.Run(tt); err != nil {
		logrus.WithError(err).WithField("command", ctx.Command).Fatal("cannot run command")
	}
}
