//go:build mage

package main

import (
	"fmt"
	"os"

	"github.com/magefile/mage/mg"
	"github.com/magefile/mage/sh"
)

var Default = All

// Metabuild rebuild the mage binary
func Metabuild() error {
	return sh.Run("go", "run", "mage.go", "-compile", "mage")
}

// Build the tt binary
func Build() error {
	return sh.Run("go", "build", "./")
}

// Run the test suite
func Test() error {
	return test(false)
}

// Run the test suite with coverage instrumentation
func Coverage() error {
	return test(true)
}

func test(coverage bool) error {
	if coverage {
		return sh.Run("go", "test", "-count", "1", "-coverprofile", "cover.out", "./...")
	} else {
		return sh.RunV("go", "test", "-count", "1", "-v", "./...")
	}
}

// Run golangci-lint v1.50.1 from its docker image on the repository
func Lint() error {
	wd, err := os.Getwd()
	if err != nil {
		return fmt.Errorf("cannot get current directory: %w", err)
	}

	err = sh.Run(
		"docker", "run", "-v", wd+":/tt", "-w", "/tt", "--rm", "golangci/golangci-lint:v1.50.1",
		"golangci-lint", "run", "./...")
	return err
}

func All() {
	mg.Deps(Build, Test)
	mg.Deps(Lint)
}
