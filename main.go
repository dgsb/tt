package main

import (
	"fmt"
	"time"

	"github.com/alecthomas/kong"
	"github.com/sirupsen/logrus"
)

type CommonConfig struct {
	Database string `name:"db" type:"file" default:"/Users/david/.tt.db" help:"the sqlite database to use for application data"`
}

type StartCmd struct {
	CommonConfig `embed:""`
}

func (cmd *StartCmd) Run() error {
	db, err := setupDB(cmd.Database)
	if err != nil {
		return fmt.Errorf("cannot setup application database: %w", err)
	}

	tt := &TimeTracker{db: db}
	if err := tt.Start(time.Now(), []string{}); err != nil {
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

func main() {

	var CLI struct {
		Start StartCmd `cmd:"" help:"start tracking a new time interval"`
		Stop  StopCmd  `cmd:"" help:"stop tracking the current opened interval"`
	}

	ctx := kong.Parse(&CLI)
	if err := ctx.Run(); err != nil {
		logrus.WithError(err).WithField("command", ctx.Command).Fatal("cannot run command")
	}
}
