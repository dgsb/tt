# Time Tracker

## What is it ?

`tt` is a time tracking tools which uses an sqlite database as the data store.
The default database used is located in this file: `${HOME}/.tt.db`.
It has been mostly inspired by [timewarrior](https://timewarrior.net/).
The goal is to:
* have a simpler user interface
* get rid of the set of json file used to hold the data
* have a single data file instead.
* have a powerful query language for reports

## Building and installing

The build process is quite simple and only requires the golang compiler.
The repository needs to be cloned first and the binary can then be installed with the `go install` command
if the go compiler has been installed properly.

```
$ git clone github.com/dgsb/tt
$ cd tt
$ go install .
```

## Usage

The database file will be built upon the first command invocation.

### basic usage

* to start a time tracking activity
```
$ tt start
```
* to stop a time tracking activity
```
$ tt stop
```
* to list the current day activities
```
$ tt list
```
* to list the current week activities (from monday to sunday)
```
$ tt list :week
```
* to start a time tracking activity with tag
```
$ tt start developement
```
* to start a time tracking activity with multiple tags
```
$ tt start developement ticket-1234
```
* to get more help
```
$ tt --help
```

Most commands takes flags to alter the start or stop time instead
of merely using the current time.

An time tracking activity doesn't need to be closed before starting a new one.
The start command will automatically close the current opened tracking activity before opening a new one.

```
$ tt start development
$ tt start meeting
$ tt start qa
```

### Manually inspecting the database

The raw content of time tracking database can be accessed directly through the sqlite3 CLI.
```
sqlite3 ${HOME}/.tt.db
```
