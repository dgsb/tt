//nolint:lll // we accept long lines for struct field tags used by kong
package main

import (
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/alecthomas/kong"
	"github.com/dgsb/configlite"
	"github.com/sirupsen/logrus"

	"github.com/dgsb/tt/internal/db"
	itime "github.com/dgsb/tt/internal/time"
)

var (
	errInvalidParameter = fmt.Errorf("invalid parameter")
)

const (
	appName = "github.com/dgsb/tt"
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
	if err := tt.StopAt(startTime); err != nil && !errors.Is(err, sql.ErrNoRows) {
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
	For time.Duration `help:"specify the stop timestamp as the wanted duration for closed interval" group:"time" xor:"time"`
}

func (cmd *StopCmd) Run(tt *db.TimeTracker) error {
	if cmd.For != 0 {
		if err := tt.StopFor(cmd.For); err != nil {
			return fmt.Errorf("cannot stop a currently opened interval: %w", err)
		}
		return nil
	}

	stopTime := time.Now()
	if !cmd.At.Time().IsZero() {
		stopTime = cmd.At.Time()
	} else if cmd.Ago != 0 {
		stopTime = time.Now().Add(-cmd.Ago)
	}

	if err := tt.StopAt(stopTime); err != nil {
		return fmt.Errorf("cannot stop a currently opened interval: %w", err)
	}

	return nil
}

type ListCmd struct {
	At     itime.Time `help:"another starting point for the required time period instead of now"`
	Tag    string     `help:"a tag to output filter on"`
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
		return fmt.Errorf("%w: time range not implemented %s", errInvalidParameter, cmd.Period)
	}

	taggedIntervals, err := tt.List(startTime, stopTime)
	if err != nil {
		return fmt.Errorf("cannot list recorded interval: %w", err)
	}

	filteredTaggedIntervals := make([]db.TaggedInterval, 0, len(taggedIntervals))
	if cmd.Tag == "" {
		filteredTaggedIntervals = taggedIntervals
	} else {
		for _, itv := range taggedIntervals {
			for _, t := range itv.Tags {
				if t == cmd.Tag {
					filteredTaggedIntervals = append(filteredTaggedIntervals, itv)
					break
				}
			}
		}
	}

	return FlatReport(filteredTaggedIntervals, os.Stdout)
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

type RecordCmd struct {
	Start itime.Time `arg:"" help:"the start time interval of the record"`
	Stop  itime.Time `arg:"" help:"the stop time interval of the record"`
	Tags  []string   `arg:"" help:"the list of tags to attach to this new interval"`
}

func (cmd *RecordCmd) Run(tt *db.TimeTracker) error {
	if err := tt.Start(cmd.Start.Time(), cmd.Tags); err != nil {
		return fmt.Errorf("cannot register new start interval: %w", err)
	}
	if err := tt.StopAt(cmd.Stop.Time()); err != nil {
		return fmt.Errorf("cannot register new stop interval: %w", err)
	}
	return nil
}

type SyncCmd struct {
	Login        string `long:"login" short:"l" help:"remote database user login"`
	Password     string `long:"password" help:"remote database password" env:"TT_SYNC_PASSWORD"`
	Hostname     string `long:"host" help:"remote database host name"`
	Port         string `long:"port" short:"p" help:"remote database connection port"`
	DatabaseName string `long:"dbname" help:"remote database name"`
}

func (cmd *SyncCmd) Run(tt *db.TimeTracker) error {

	repo, err := configlite.New(configlite.DefaultConfigurationFile())
	if err != nil {
		return fmt.Errorf("cannot open configuration repository: %w", err)
	}

	if cmd.Login == "" {
		cmd.Login, err = repo.GetConfig(appName, "syncer_login")
	}

	if cmd.Password == "" && err == nil {
		cmd.Password, err = repo.GetConfig(appName, "syncer_password")
	}

	if cmd.Hostname == "" && err == nil {
		cmd.Hostname, err = repo.GetConfig(appName, "syncer_hostname")
	}

	if cmd.Port == "" && err == nil {
		cmd.Port, err = repo.GetConfig(appName, "syncer_port")
	}

	var portInt int
	if err == nil {
		portInt, err = strconv.Atoi(cmd.Port)
	}

	if cmd.DatabaseName == "" && err == nil {
		cmd.DatabaseName, err = repo.GetConfig(appName, "syncer_databasename")
	}

	if err == nil {
		err = tt.Sync(db.SyncerConfig{
			Login:        cmd.Login,
			Password:     cmd.Password,
			Hostname:     cmd.Hostname,
			Port:         portInt,
			DatabaseName: cmd.DatabaseName,
		})
	}

	return err
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
		Record   RecordCmd   `cmd:"" help:"record a new closed interval with it tags"`
		Start    StartCmd    `cmd:"" help:"start tracking a new time interval"`
		Stop     StopCmd     `cmd:"" help:"stop tracking the current opened interval"`
		Sync     SyncCmd     `cmd:"" help:"synchronise with remote central database"`
		Tag      TagCmd      `cmd:"" help:"tag an interval with given values"`
		Untag    UntagCmd    `cmd:"" help:"remove tags from an interval"`
		Vacuum   VacuumCmd   `cmd:"" help:"hard delete old soft deleted data"`
	}

	ctx := kong.Parse(&CLI, kong.Vars{"home": homeDir})

	tt, err := db.New(CLI.CommonConfig.Database)
	if err != nil {
		logrus.WithError(err).Fatal("cannot setup application database")
	}
	defer func() {
		if err := tt.Close(); err != nil {
			logrus.WithError(err).Fatal("cannot close TimeTracker object")
		}
	}()

	if err := ctx.Run(tt); err != nil {
		logrus.WithError(err).WithField("command", ctx.Command).Fatal("cannot run command")
	}
}
