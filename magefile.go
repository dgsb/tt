//go:build mage

package main

import (
	"fmt"
	"os"

	"github.com/magefile/mage/sh"
)

func Test() error {
	wd, err := os.Getwd()
	fmt.Println(wd)
	return err
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
