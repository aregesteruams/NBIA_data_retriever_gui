package app

import (
	"context"
	"fmt"
	"os/exec"

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

// RunCLIFetch runs the CLI tool with the given manifest and output directory and advanced options
func (b *App) RunCLIFetch(manifestPath string, outputDir string, maxConnections int, maxRetries int, simultaneousDownloads int, skipExisting bool, downloadInParallel bool) (string, error) {
	cliPath := "../nbia-data-retriever-cli"
	args := []string{"-i", manifestPath, "--output", outputDir,
		"--max-connections", fmt.Sprintf("%d", maxConnections),
		"--max-retries", fmt.Sprintf("%d", maxRetries),
		"--processes", fmt.Sprintf("%d", simultaneousDownloads),
	}
	if skipExisting {
		args = append(args, "--skip-existing")
	}

	if downloadInParallel {
		args = append(args, "--download-in-parallel")
	}

	cmd := exec.Command(cliPath, args...)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return string(output), err
	}
	return string(output), nil
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

func (b *App) Startup(ctx context.Context) {
	b.ctx = ctx
}

func (b *App) Shutdown(ctx context.Context) {
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
