package main

import (
	"context"
	"fmt"
	"io"
	"os"

	"github.com/GrigoryEvko/NBIA_data_retriever_CLI/downloader"
	"github.com/wailsapp/wails/v2/pkg/runtime"
)

// OpenInputFileDialog opens a system file dialog and returns the selected file path
func (b *App) OpenInputFileDialog() (string, error) {
	result, err := runtime.OpenFileDialog(b.ctx, runtime.OpenDialogOptions{
		Title: "Select TCIA Manifest File",
		Filters: []runtime.FileFilter{
			{DisplayName: "TCIA Manifest Files", Pattern: "*.tcia"},
			{DisplayName: "All Files", Pattern: "*"},
		},
	})
	if err != nil {
		return "", err
	}
	if result == "" {
		return "", nil // User cancelled
	}
	return result, nil
}

// OpenOutputDirectoryDialog opens a system directory dialog and returns the selected directory path
func (b *App) OpenOutputDirectoryDialog() (string, error) {
	result, err := runtime.OpenDirectoryDialog(b.ctx, runtime.OpenDialogOptions{
		Title: "Download Directory",
	})
	if err != nil {
		return "", err
	}
	if result == "" {
		return "", nil // User cancelled
	}
	return result, nil
}

// RunFetch runs the CLI tool with the given manifest and output directory and advanced options
func (b *App) RunFetch(manifestPath string, outputDir string, maxConnections int, maxRetries int, simultaneousDownloads int, skipExisting bool, downloadInParallel bool) {
	r, w, _ := os.Pipe()
	os.Stdout = w
	os.Stderr = w

	go func() {
		defer r.Close()
		buf := make([]byte, 1024)
		for {
			n, err := r.Read(buf)
			if err != nil {
				if err == io.EOF {
					break
				}
				fmt.Println(err)
				break
			}
			runtime.EventsEmit(b.ctx, "output", string(buf[:n]))
		}
	}()

	go func() {
		options := &downloader.Options{
			Input:           manifestPath,
			Output:          outputDir,
			MaxConnsPerHost: maxConnections,
			MaxRetries:      maxRetries,
			Concurrent:      simultaneousDownloads,
			SkipExisting:    skipExisting,
			DownloadInParallel: downloadInParallel,
		}
		downloader.Download(b.ctx, options, func(eventName string, data ...interface{}) {
			runtime.EventsEmit(b.ctx, eventName, data...)
		})
		w.Close()
	}()
}

type App struct {
	ctx context.Context
}

func NewApp() *App {
	return &App{}
}

func (a *App) FetchFiles() string {
	return "Done!"
}

func (b *App) startup(ctx context.Context) {
	b.ctx = ctx
}

func (b *App) shutdown(ctx context.Context) {
	// Perform teardown here
}

func (b *App) Greet(name string) string {
	return fmt.Sprintf("Hello %s, It's show time!", name)
}

func (b *App) ShowDialog() {
	_, err := runtime.MessageDialog(b.ctx, runtime.MessageDialogOptions{
		Type:    runtime.InfoDialog,
		Title:   "Native Dialog from Go",
		Message: "This is a Native Dialog send from Go.",
	})

	if err != nil {
		panic(err)
	}
}
