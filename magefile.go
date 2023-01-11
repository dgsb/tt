//go:build mage

package main

import (
	"fmt"
	"os"

	"github.com/magefile/mage/mg"
	"github.com/magefile/mage/sh"
)

var Default = All

// Build the tt binary
func Build() error {
	return sh.Run("go", "build", "./")
}

// Run the test suite
func Test() error {
	return sh.Run("go", "test", "./...")
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

func Failed() error {
	return fmt.Errorf("failed")
}

func All() {
	mg.Deps(Build, Test)
	mg.Deps(Lint)
}
