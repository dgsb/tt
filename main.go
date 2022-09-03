package main

import (
	"fmt"
	"os"
	"time"

	"github.com/alecthomas/kong"
	"github.com/sirupsen/logrus"
)

type CommonConfig struct {
	Database string `name:"db" type:"file" default:"${home}/.tt.db" help:"the sqlite database to use for application data"`
}

type StartCmd struct {
	At   time.Time     `help:"specify the stop timestamp in RFC3339 format" group:"time" xor:"time"`
	Ago  time.Duration `help:"specify the stop timestamp as a duration in the past" group:"time" xor:"time"`
	Tags []string      `arg:"" optional:"" help:"the value to tag the interval with"`
}

func (cmd *StartCmd) Run(cfg *CommonConfig) error {
	db, err := setupDB(cfg.Database)
	if err != nil {
		return fmt.Errorf("cannot setup application database: %w", err)
	}

	startTime := time.Now()
	if !cmd.At.IsZero() {
		startTime = cmd.At
	} else if cmd.Ago != 0 {
		startTime = time.Now().Add(-cmd.Ago)
	}

	tt := &TimeTracker{db: db}
	if err := tt.Start(startTime, cmd.Tags); err != nil {
		return fmt.Errorf("cannot start a new opened interval: %w", err)
	}

	return nil
}

type StopCmd struct {
	At  time.Time     `help:"specify the stop timestamp in RFC3339 format" group:"time" xor:"time"`
	Ago time.Duration `help:"specify the stop timestamp as a duration in the past" group:"time" xor:"time"`
}

func (cmd *StopCmd) Run(cfg *CommonConfig) error {

	db, err := setupDB(cfg.Database)
	if err != nil {
		return fmt.Errorf("cannot setup application database: %w", err)
	}

	tt := &TimeTracker{db: db}

	stopTime := time.Now()
	if !cmd.At.IsZero() {
		stopTime = cmd.At
	} else if cmd.Ago != 0 {
		stopTime = time.Now().Add(-cmd.Ago)
	}

	if err := tt.Stop(stopTime); err != nil {
		return fmt.Errorf("cannot stop a currently opened interval: %w", err)
	}

	return nil
}

type ListCmd struct {
}

func (cmd *ListCmd) Run(cfg *CommonConfig) error {
	db, err := setupDB(cfg.Database)
	if err != nil {
		return fmt.Errorf("cannot setup application database: %w", err)
	}

	tt := &TimeTracker{db: db}
	taggedIntervals, err := tt.List(time.Now().Add(-time.Hour * 24 * 365))
	if err != nil {
		return fmt.Errorf("cannot list recorded interval: %w", err)
	}

	for _, ti := range taggedIntervals {
		fmt.Println(ti)
	}

	return nil
}

type DeleteCmd struct {
	IDs []string `arg:"" name:"ids" help:"the ids of the intervals to delete"`
}

func (cmd *DeleteCmd) Run(cfg *CommonConfig) error {
	db, err := setupDB(cfg.Database)
	if err != nil {
		return fmt.Errorf("cannot setup application database: %w", err)
	}

	tt := &TimeTracker{db: db}
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

func (cmd *TagCmd) Run(cfg *CommonConfig) error {
	db, err := setupDB(cfg.Database)
	if err != nil {
		return fmt.Errorf("cannot setup application database: %w", err)
	}

	tt := &TimeTracker{db: db}

	if err := tt.Tag(cmd.ID, cmd.Tags); err != nil {
		return fmt.Errorf("cannot tag interval %s with %s: %w", cmd.ID, cmd.Tags, err)
	}

	return nil
}

type UntagCmd struct {
	ID   string   `arg:"" help:"the interval id to untag"`
	Tags []string `arg:"" help:"the tag to remove from the interval"`
}

func (cmd *UntagCmd) Run(cfg *CommonConfig) error {
	db, err := setupDB(cfg.Database)
	if err != nil {
		return fmt.Errorf("cannot setup application database: %w", err)
	}

	tt := &TimeTracker{db: db}

	if err := tt.Untag(cmd.ID, cmd.Tags); err != nil {
		return fmt.Errorf("cannot untag %s from %s: %w", cmd.ID, cmd.Tags, err)
	}
	return nil
}

type CurrentCmd struct {
}

func (cmd *CurrentCmd) Run(cfg *CommonConfig) error {
	db, err := setupDB(cfg.Database)
	if err != nil {
		return fmt.Errorf("cannot setup application database: %w", err)
	}

	tt := &TimeTracker{db: db}

	interval, err := tt.Current()
	if err != nil {
		return fmt.Errorf("cannot retrieve current interval: %w", err)
	}
	if interval != nil {
		fmt.Println(*interval)
	}
	return nil
}

type ContinueCmd struct{}

func (cmd *ContinueCmd) Run(cfg *CommonConfig) error {
	db, err := setupDB(cfg.Database)
	if err != nil {
		return fmt.Errorf("cannot setup application database: %w", err)
	}

	tt := &TimeTracker{db: db}

	if err := tt.Continue(time.Now()); err != nil {
		return fmt.Errorf("cannot continue a previously closed interval: %w", err)
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
	}

	ctx := kong.Parse(&CLI, kong.Vars{"home": homeDir})
	if err := ctx.Run(&CLI.CommonConfig); err != nil {
		logrus.WithError(err).WithField("command", ctx.Command).Fatal("cannot run command")
	}
}
