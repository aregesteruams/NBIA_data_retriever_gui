package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/GrigoryEvko/NBIA_data_retriever_CLI/downloader"
)

var (
	// version and build info
	buildStamp string
	gitHash    string
	goVersion  string
	version    string
)

// SetupCloseHandler creates a 'listener' on a new goroutine which will notify the
// program if it receives an interrupt from the OS. We then handle this by calling
// our clean-up procedure and exiting the program.
func setupCloseHandler() {
	c := make(chan os.Signal, 2)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		fmt.Println("\r- Ctrl+C pressed in Terminal")
		os.Exit(0)
	}()
}

func main() {
	setupCloseHandler()

	options := downloader.NewOptions()

	if options.Version {
		downloader.Logger.Infof("Current version: %s", version)
		downloader.Logger.Infof("Git Commit Hash: %s", gitHash)
		downloader.Logger.Infof("UTC Build Time : %s", buildStamp)
		downloader.Logger.Infof("Golang Version : %s", goVersion)
		os.Exit(0)
	} else {
		downloader.Download(nil, options, nil)
	}
}
