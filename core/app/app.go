package app

import (
	"context"
	"fmt"
	"sync"

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
	opts := RunOptions{
		MaxConnections:     maxConnections,
		MaxRetries:         maxRetries,
		Processes:          simultaneousDownloads,
		SkipExisting:       skipExisting,
		DownloadInParallel: downloadInParallel,
	}

	// Create a cancellable child context for this run so GUI can cancel it
	ctx, cancel := context.WithCancel(b.ctx)

	// Store cancel func so other methods can cancel the run
	b.runMu.Lock()
	b.runCancel = cancel
	b.runMu.Unlock()

	// Ensure we clear the cancel func when done
	defer func() {
		b.runMu.Lock()
		b.runCancel = nil
		b.runMu.Unlock()
	}()

	// Forward logs from the retriever to the frontend runtime events
	logFn := func(line string) {
		runtime.EventsEmit(b.ctx, "cli-output-line", line)
	}

	// Hook up token provider from GUI if set
	getToken := func() (string, error) {
		// read token from App state if available
		b.runMu.Lock()
		defer b.runMu.Unlock()
		// accessToken stored in runCancel? we will add accessToken field
		if b.accessToken != "" {
			return b.accessToken, nil
		}
		return "", nil
	}

	return Run(ctx, manifestPath, outputDir, opts, getToken, logFn)
}

// CancelRun cancels any currently running CLIFetch started via RunCLIFetch.
func (b *App) CancelRun() error {
	b.runMu.Lock()
	cancel := b.runCancel
	b.runMu.Unlock()

	if cancel == nil {
		return fmt.Errorf("no active run to cancel")
	}
	cancel()
	// Emit a notice to the frontend
	runtime.EventsEmit(b.ctx, "cli-output-line", "[GUI] Cancel requested")
	return nil
}

type App struct {
	ctx context.Context
	// runCancel holds the cancel function for the currently running fetch (if any)
	runCancel context.CancelFunc
	runMu     sync.Mutex
	// accessToken is an optional OAuth token provided by the GUI
	accessToken string
	tokenMu     sync.Mutex
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

// SetAccessToken stores an access token that will be used by retriever.Run
// when making authenticated requests. Call from the frontend when a token is
// available (e.g., from a login flow).
func (b *App) SetAccessToken(token string) error {
	b.tokenMu.Lock()
	b.accessToken = token
	b.tokenMu.Unlock()
	runtime.EventsEmit(b.ctx, "cli-output-line", "[GUI] Access token set")
	return nil
}

// ClearAccessToken clears any previously set access token.
func (b *App) ClearAccessToken() error {
	b.tokenMu.Lock()
	b.accessToken = ""
	b.tokenMu.Unlock()
	runtime.EventsEmit(b.ctx, "cli-output-line", "[GUI] Access token cleared")
	return nil
}
