package main

import (
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
		return app.DecodeTCIA(filePath, client, token, options), nil
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

		var wg sync.WaitGroup
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

		stats := &DownloadStats{Total: int32(len(files))}

		// Initialize progress tracking
		stats.StartTime = time.Now()
		if options.Debug {
			logger.Infof("Starting download of %d series with %d workers", len(files), options.Concurrent)
		} else {
			fmt.Fprintf(os.Stderr, "\nDownloading %d series with %d workers...\n\n", len(files), options.Concurrent)
		}

		wg.Add(options.Concurrent)
		inputChan := make(chan *app.FileInfo, len(files)) // Larger buffer to prevent blocking

		// Create worker contexts
		for i := 0; i < options.Concurrent; i++ {
			ctx := &WorkerContext{
				HTTPClient: client,
				AuthToken:  token,
				Options:    options,
				Stats:      stats,
				WorkerID:   i + 1,
			}

			go func(ctx *WorkerContext, input chan *app.FileInfo) {
				defer wg.Done()
				for fileInfo := range input {
					// Update progress display
					updateProgress(ctx.Stats, fileInfo.SeriesUID, ctx.Options.Debug)
					logger.Debugf("[Worker %d] Processing %s", ctx.WorkerID, fileInfo.SeriesUID)

					// Skip metadata saving for spreadsheet inputs
					isSpreadsheetInput := fileInfo.DownloadURL != ""

					if ctx.Options.Meta {
						if isSpreadsheetInput {
							// For spreadsheets, --meta is a no-op, just skip.
							logger.Debugf("[Worker %d] Skipping metadata for spreadsheet entry %s", ctx.WorkerID, fileInfo.SeriesUID)
							atomic.AddInt32(&ctx.Stats.Skipped, 1)
						} else {
							// Only download metadata for TCIA inputs
							if err := fileInfo.GetMeta(ctx.Options.Output); err != nil {
								logger.Warnf("[Worker %d] Save meta info %s failed - %s", ctx.WorkerID, fileInfo.SeriesUID, err)
								atomic.AddInt32(&ctx.Stats.Failed, 1)
							} else {
								atomic.AddInt32(&ctx.Stats.Downloaded, 1)
							}
						}
						updateProgress(ctx.Stats, fileInfo.SeriesUID, ctx.Options.Debug)
					} else {
						// Download images (and save metadata for TCIA inputs)
						if ctx.Options.SkipExisting && !fileInfo.NeedsDownload(ctx.Options.Output, false, ctx.Options.NoDecompress) {
							logger.Debugf("[Worker %d] Skip existing %s", ctx.WorkerID, fileInfo.SeriesUID)
							atomic.AddInt32(&ctx.Stats.Skipped, 1)
							updateProgress(ctx.Stats, fileInfo.SeriesUID, ctx.Options.Debug)
							continue
						}

						if fileInfo.NeedsDownload(ctx.Options.Output, ctx.Options.Force, ctx.Options.NoDecompress) {
							if err := fileInfo.Download(ctx.Options.Output, ctx.HTTPClient, ctx.AuthToken, ctx.Options); err != nil {
								logger.Warnf("[Worker %d] Download %s failed - %s", ctx.WorkerID, fileInfo.SeriesUID, err)
								atomic.AddInt32(&ctx.Stats.Failed, 1)
							} else {
								// Save metadata only for TCIA inputs
								if !isSpreadsheetInput {
									if err := fileInfo.GetMeta(ctx.Options.Output); err != nil {
										logger.Warnf("[Worker %d] Save meta info %s failed - %s", ctx.WorkerID, fileInfo.SeriesUID, err)
									}
								}
								atomic.AddInt32(&ctx.Stats.Downloaded, 1)
							}
							updateProgress(ctx.Stats, fileInfo.SeriesUID, ctx.Options.Debug)
						} else {
							logger.Debugf("[Worker %d] Skip %s (already exists with correct size/checksum)", ctx.WorkerID, fileInfo.SeriesUID)
							atomic.AddInt32(&ctx.Stats.Skipped, 1)
							updateProgress(ctx.Stats, fileInfo.SeriesUID, ctx.Options.Debug)
						}
					}
				}
			}(ctx, inputChan)
		}

		for _, f := range files {
			inputChan <- f
		}
		close(inputChan)
		wg.Wait()

		// Final progress update
		updateProgress(stats, "Complete", options.Debug)

		// Clear progress line in non-debug mode
		if !options.Debug {
			fmt.Fprintf(os.Stderr, "\n")
		}

		elapsed := time.Since(stats.StartTime)
		fmt.Println("\n=== Download Summary ===")
		fmt.Printf("Total files: %d\n", stats.Total)
		fmt.Printf("Downloaded: %d\n", stats.Downloaded)
		fmt.Printf("Skipped: %d\n", stats.Skipped)
		fmt.Printf("Failed: %d\n", stats.Failed)
		fmt.Printf("Total time: %s\n", elapsed.Round(time.Second))

		if stats.Total > 0 {
			rate := float64(stats.Downloaded+stats.Skipped) / elapsed.Seconds()
			fmt.Printf("Average rate: %.1f files/second\n", rate)
		}

		if stats.Failed > 0 {
			logger.Warnf("Some downloads failed. Check the logs above for details.")
		}
	}
}
