package main

import (
	"context"
	"fmt"
	"go.uber.org/zap"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/aregesteruams/NBIA_data_retriever_gui/core/app"
)

var (
	// version and build info
	buildStamp string
	gitHash    string
	goVersion  string
	version    string
	client     *http.Client
	token      *app.Token
	logger     *zap.SugaredLogger
)

// DownloadStats tracks download statistics
type DownloadStats struct {
	Total          int32
	Downloaded     int32
	Skipped        int32
	Failed         int32
	StartTime      time.Time
	LastUpdate     time.Time
	LastPercentage int
	mu             sync.Mutex
}

// WorkerContext contains all dependencies for workers
type WorkerContext struct {
	HTTPClient *http.Client
	AuthToken  *app.Token
	Options    *app.Options
	Stats      *DownloadStats
	WorkerID   int
}

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

// decodeInputFile determines the input file type and calls the appropriate decoder
func decodeInputFile(filePath string, client *http.Client, token *app.Token, options *app.Options) ([]*app.FileInfo, error) {
	ext := strings.ToLower(filepath.Ext(filePath))
	switch ext {
	case ".tcia":
		// Use the retriever package to fetch metadata for TCIA manifests.
		// Map main.Options to app.RunOptions for metadata fetching.
		runOpts := app.RunOptions{
			MaxConnections:  options.MaxConnsPerHost,
			MaxRetries:      options.MaxRetries,
			Processes:       options.Concurrent,
			SkipExisting:    options.SkipExisting,
			MetadataWorkers: options.MetadataWorkers,
			RefreshMetadata: options.RefreshMetadata,
			MetaUrl:         options.MetaUrl,
		}

		// Use token.GetAccessToken as the getter function
		getToken := func() (string, error) {
			if token == nil {
				return "", fmt.Errorf("no token available")
			}
			return token.GetAccessToken()
		}

		rfiles, err := app.FetchMetadata(context.Background(), filePath, options.Output, client, getToken, runOpts, func(line string) {
			// Forward retriever logs to stderr (preserves original CLI behavior)
			fmt.Fprintln(os.Stderr, line)
		})
		if err != nil {
			return nil, err
		}

		return rfiles, nil
	case ".csv", ".tsv", ".xlsx":
		return app.DecodeSpreadsheet(filePath)
	default:
		return nil, fmt.Errorf("unsupported input file format: %s", ext)
	}
}

// updateProgress prints the current download progress
func updateProgress(stats *DownloadStats, currentSeriesID string, debugMode bool) {
	stats.mu.Lock()
	defer stats.mu.Unlock()

	now := time.Now()

	// Update at most once per 200ms for smooth updates
	if now.Sub(stats.LastUpdate) < 200*time.Millisecond {
		return
	}
	stats.LastUpdate = now

	// Calculate progress
	processed := atomic.LoadInt32(&stats.Downloaded) + atomic.LoadInt32(&stats.Skipped) + atomic.LoadInt32(&stats.Failed)
	percentage := float64(processed) / float64(stats.Total) * 100

	// Calculate ETA based on download rate only
	elapsed := time.Since(stats.StartTime)
	var eta string
	if stats.Downloaded > 0 && elapsed > 0 {
		rate := float64(stats.Downloaded) / elapsed.Seconds()
		remainingFiles := float64(stats.Total - stats.Downloaded - stats.Skipped - stats.Failed)
		if remainingFiles > 0 && rate > 0 {
			remainingTime := remainingFiles / rate
			etaDuration := time.Duration(remainingTime * float64(time.Second))
			eta = fmt.Sprintf(" | ETA: %s", etaDuration.Round(time.Second))
		}
	}

	// Truncate series ID for display
	displayID := currentSeriesID
	if len(displayID) > 30 {
		displayID = displayID[:30] + "..."
	}

	// Clear line and print progress
	fmt.Fprintf(os.Stderr, "\r\033[K[%d/%d] %.1f%% | Downloaded: %d | Skipped: %d | Failed: %d%s | Current: %s",
		processed, stats.Total, percentage,
		stats.Downloaded, stats.Skipped, stats.Failed,
		eta, displayID)
}

func main() {
	setupCloseHandler()

	var options = app.InitOptions()
	logger := app.Logger

	if options.Version {
		logger.Infof("Current version: %s", version)
		logger.Infof("Git Commit Hash: %s", gitHash)
		logger.Infof("UTC Build Time : %s", buildStamp)
		logger.Infof("Golang Version : %s", goVersion)
		os.Exit(0)
	} else {
		client = app.NewClient(options.Proxy, options.MaxConnsPerHost)

		err := os.MkdirAll(options.Output, os.ModePerm)
		if err != nil {
			logger.Fatalf("failed to create output directory: %v", err)
		}
		token, err = app.NewToken(
			options.Username, options.Password,
			filepath.Join(options.Output, fmt.Sprintf("%s.json", options.Username)))

		if err != nil {
			logger.Fatal(err)
		}

		// Create metadata directory
		if err := app.CreateMetadataDir(options.Output); err != nil {
			logger.Fatalf("Failed to create metadata directory: %v", err)
		}

		files, err := decodeInputFile(options.Input, client, token, options)
		if err != nil {
			logger.Fatalf("Failed to decode input file: %v", err)
		}

		// If input is a spreadsheet, copy it to the metadata folder
		ext := strings.ToLower(filepath.Ext(options.Input))
		if ext == ".csv" || ext == ".tsv" || ext == ".xlsx" {
			metaDir := filepath.Join(options.Output, "metadata")
			if err := os.MkdirAll(metaDir, 0755); err != nil {
				logger.Fatalf("Failed to create metadata directory: %v", err)
			}
			destPath := filepath.Join(metaDir, filepath.Base(options.Input))
			if err := app.CopyFile(options.Input, destPath); err != nil {
				logger.Warnf("Failed to copy spreadsheet to metadata folder: %v", err)
			}
		}

		// Use retriever orchestrator to process downloads (incremental refactor).
		// Build adapters so retriever can call back into existing app.FileInfo methods.
		adapters := make([]app.Downloadable, 0, len(files))
		for _, f := range files {
			// Create an adapter that captures the necessary dependencies
			ad := &downloadAdapter{
				fi:         f,
				httpClient: client,
				authToken:  token,
				options:    options,
			}
			adapters = append(adapters, ad)
		}

		metaOnly := options.Meta
		runOpts := app.RunOptions{
			Processes:    options.Concurrent,
			SkipExisting: options.SkipExisting,
		}

		summary, err := app.OrchestrateDownloads(context.Background(), adapters, options.Output, runOpts, metaOnly, func(line string) {
			// Forward logs to stderr to preserve CLI behavior
			fmt.Fprintln(os.Stderr, line)
		})

		if err != nil {
			logger.Warnf("Orchestrator returned error: %v", err)
		}

		// Print summary similar to previous behavior
		fmt.Println("\n=== Download Summary ===")
		fmt.Printf("Total files: %d\n", summary.Total)
		fmt.Printf("Downloaded: %d\n", summary.Downloaded)
		fmt.Printf("Skipped: %d\n", summary.Skipped)
		fmt.Printf("Failed: %d\n", summary.Failed)
		fmt.Printf("Total time: %s\n", summary.Elapsed.Round(time.Second))

		if summary.Total > 0 {
			rate := float64(summary.Downloaded+summary.Skipped) / summary.Elapsed.Seconds()
			fmt.Printf("Average rate: %.1f files/second\n", rate)
		}

		if summary.Failed > 0 {
			logger.Warnf("Some downloads failed. Check the logs above for details.")
		}
	}
}

// downloadAdapter adapts the app.FileInfo methods into the app.Downloadable interface.
type downloadAdapter struct {
	fi         *app.FileInfo
	httpClient *http.Client
	authToken  *app.Token
	options    *app.Options
}

func (d *downloadAdapter) NeedsDownload(output string) bool {
	return d.fi.NeedsDownload(output, d.options.Force, d.options.NoDecompress)
}

func (d *downloadAdapter) Download(ctx context.Context, output string) error {
	return d.fi.Download(ctx, output, d.httpClient, d.authToken, d.options)
}

func (d *downloadAdapter) GetMeta(output string) error {
	return d.fi.GetMeta(output)
}

func (d *downloadAdapter) SeriesID() string {
	return d.fi.SeriesUID
}
