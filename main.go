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
	CommonConfig `embed:""`
	Tags         []string `arg:"" optional:"" help:"the value to tag the interval with"`
}

func (cmd *StartCmd) Run() error {
	db, err := setupDB(cmd.Database)
	if err != nil {
		return fmt.Errorf("cannot setup application database: %w", err)
	}

	tt := &TimeTracker{db: db}
	if err := tt.Start(time.Now(), cmd.Tags); err != nil {
		return fmt.Errorf("cannot start a new opened interval: %w", err)
	}

	return nil
}

type StopCmd struct {
	CommonConfig `embed:""`
}

func (cmd *StopCmd) Run() error {

	db, err := setupDB(cmd.Database)
	if err != nil {
		return fmt.Errorf("cannot setup application database: %w", err)
	}

	tt := &TimeTracker{db: db}
	if err := tt.Stop(time.Now()); err != nil {
		return fmt.Errorf("cannot stop a currently opened interval: %w", err)
	}

	return nil
}

type ListCmd struct {
	CommonConfig `embed:""`
}

func (cmd *ListCmd) Run() error {
	db, err := setupDB(cmd.Database)
	if err != nil {
		return fmt.Errorf("cannot setup application database: %w", err)
	}

	tt := &TimeTracker{db: db}
	taggedInterval, err := tt.List(time.Now().Add(-time.Hour * 24 * 365))
	if err != nil {
		return fmt.Errorf("cannot list recorded interval: %w", err)
	}

	fmt.Println(taggedInterval)

	return nil
}

type DeleteCmd struct {
	CommonConfig `embed:""`
	IDs          []string `arg:"" help:"the ids of the intervals to delete"`
}

func (cmd *DeleteCmd) Run() error {
	db, err := setupDB(cmd.Database)
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
	CommonConfig `embed:""`
	ID           string   `args:"" help:"the interval id to tag"`
	Tags         []string `args:"" help:"values to tag the interval with"`
}

func (cmd *TagCmd) Run() error {
	db, err := setupDB(cmd.Database)
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
	CommonConfig `embed:""`
	ID           string   `args:"" help:"the interval id to untag"`
	Tags         []string `args:"" help:"the tag to remove from the interval"`
}

func (cmd *UntagCmd) Run() error {
	db, err := setupDB(cmd.Database)
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
	CommonConfig `embed:""`
}

func (cmd *CurrentCmd) Run() error {
	db, err := setupDB(cmd.Database)
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

func main() {

	homeDir, err := os.UserHomeDir()
	if err != nil {
		logrus.WithError(err).Fatal("cannot retrieve user home directory")
	}

	var CLI struct {
		Start   StartCmd   `cmd:"" help:"start tracking a new time interval"`
		Stop    StopCmd    `cmd:"" help:"stop tracking the current opened interval"`
		List    ListCmd    `cmd:"" help:"list intervals"`
		Delete  DeleteCmd  `cmd:"" help:"delete a registered interval"`
		Tag     TagCmd     `cmd:"" help:"tag an interval with given values"`
		Untag   UntagCmd   `cmd:"" help:"remove tags from an interval"`
		Current CurrentCmd `default:"1" cmd:"" help:"return the current opened interval"`
	}

	ctx := kong.Parse(&CLI, kong.Vars{"home": homeDir})
	if err := ctx.Run(); err != nil {
		logrus.WithError(err).WithField("command", ctx.Command).Fatal("cannot run command")
	}
}
