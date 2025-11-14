package downloader

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// updateProgress prints the current download progress
func updateProgress(ctx context.Context, stats *DownloadStats, currentSeriesID string, debugMode bool, eventsEmit func(string, ...interface{})) {
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

	if eventsEmit != nil {
		eventsEmit("progress", map[string]interface{}{
			"processed":  processed,
			"total":      stats.Total,
			"percentage": percentage,
			"downloaded": stats.Downloaded,
			"skipped":    stats.Skipped,
			"failed":     stats.Failed,
			"eta":        eta,
			"currentId":  displayID,
		})
	} else {
		// Clear line and print progress
		fmt.Fprintf(os.Stderr, "\r\033[K[%d/%d] %.1f%% | Downloaded: %d | Skipped: %d | Failed: %d%s | Current: %s",
			processed, stats.Total, percentage,
			stats.Downloaded, stats.Skipped, stats.Failed,
			eta, displayID)
	}
}

// decodeInputFile determines the input file type and calls the appropriate decoder
func decodeInputFile(filePath string, client *http.Client, token *Token, options *Options) ([]*FileInfo, error) {
	ext := strings.ToLower(filepath.Ext(filePath))
	switch ext {
	case ".tcia":
		return DecodeTCIA(filePath, client, token, options), nil
	case ".csv", ".tsv", ".xlsx":
		return DecodeSpreadsheet(filePath)
	default:
		return nil, fmt.Errorf("unsupported input file format: %s", ext)
	}
}

func Download(ctx context.Context, options *Options, eventsEmit func(string, ...interface{})) {
	client := newClient(options.Proxy, options.MaxConnsPerHost)

	err := os.MkdirAll(options.Output, os.ModePerm)
	if err != nil {
		Logger.Fatalf("failed to create output directory: %v", err)
	}
	token, err := NewToken(client,
		options.Username, options.Password,
		filepath.Join(options.Output, fmt.Sprintf("%s.json", options.Username)))

	if err != nil {
		Logger.Fatal(err)
	}

	// Create metadata directory
	if err := createMetadataDir(options.Output); err != nil {
		Logger.Fatalf("Failed to create metadata directory: %v", err)
	}

	var wg sync.WaitGroup
	files, err := decodeInputFile(options.Input, client, token, options)
	if err != nil {
		Logger.Fatalf("Failed to decode input file: %v", err)
	}

	// If input is a spreadsheet, copy it to the metadata folder
	ext := strings.ToLower(filepath.Ext(options.Input))
	if ext == ".csv" || ext == ".tsv" || ext == ".xlsx" {
		metaDir := filepath.Join(options.Output, "metadata")
		if err := os.MkdirAll(metaDir, 0755); err != nil {
			Logger.Fatalf("Failed to create metadata directory: %v", err)
		}
		destPath := filepath.Join(metaDir, filepath.Base(options.Input))
		if err := CopyFile(options.Input, destPath); err != nil {
			Logger.Warnf("Failed to copy spreadsheet to metadata folder: %v", err)
		}
	}

	stats := &DownloadStats{Total: int32(len(files))}

	// Initialize progress tracking
	stats.StartTime = time.Now()
	if options.Debug {
		Logger.Infof("Starting download of %d series with %d workers", len(files), options.Concurrent)
	} else {
		fmt.Fprintf(os.Stderr, "\nDownloading %d series with %d workers...\n\n", len(files), options.Concurrent)
	}

	concurrent := options.Concurrent
	if !options.DownloadInParallel {
		concurrent = 1
	}
	wg.Add(concurrent)
	inputChan := make(chan *FileInfo, len(files)) // Larger buffer to prevent blocking

	// Create worker contexts
	for i := 0; i < concurrent; i++ {
		ctx := &WorkerContext{
			HTTPClient: client,
			AuthToken:  token,
			Options:    options,
			Stats:      stats,
			WorkerID:   i + 1,
		}

		go func(ctx *WorkerContext, input chan *FileInfo) {
			defer wg.Done()
			for fileInfo := range input {
				// Update progress display
				updateProgress(nil, ctx.Stats, fileInfo.SeriesUID, ctx.Options.Debug, eventsEmit)
				Logger.Debugf("[Worker %d] Processing %s", ctx.WorkerID, fileInfo.SeriesUID)

				// Skip metadata saving for spreadsheet inputs
				isSpreadsheetInput := fileInfo.DownloadURL != ""

				if ctx.Options.Meta {
					if isSpreadsheetInput {
						// For spreadsheets, --meta is a no-op, just skip.
						Logger.Debugf("[Worker %d] Skipping metadata for spreadsheet entry %s", ctx.WorkerID, fileInfo.SeriesUID)
						atomic.AddInt32(&ctx.Stats.Skipped, 1)
					} else {
						// Only download metadata for TCIA inputs
						if err := fileInfo.GetMeta(ctx.Options.Output); err != nil {
							Logger.Warnf("[Worker %d] Save meta info %s failed - %s", ctx.WorkerID, fileInfo.SeriesUID, err)
							atomic.AddInt32(&ctx.Stats.Failed, 1)
						} else {
							atomic.AddInt32(&ctx.Stats.Downloaded, 1)
						}
					}
					updateProgress(nil, ctx.Stats, fileInfo.SeriesUID, ctx.Options.Debug, eventsEmit)
				} else {
					// Download images (and save metadata for TCIA inputs)
					if ctx.Options.SkipExisting && !fileInfo.NeedsDownload(ctx.Options.Output, false, ctx.Options.NoDecompress) {
						Logger.Debugf("[Worker %d] Skip existing %s", ctx.WorkerID, fileInfo.SeriesUID)
						atomic.AddInt32(&ctx.Stats.Skipped, 1)
						updateProgress(nil, ctx.Stats, fileInfo.SeriesUID, ctx.Options.Debug, eventsEmit)
						continue
					}

					if fileInfo.NeedsDownload(ctx.Options.Output, ctx.Options.Force, ctx.Options.NoDecompress) {
						if err := fileInfo.Download(ctx.Options.Output, ctx.HTTPClient, ctx.AuthToken, ctx.Options); err != nil {
							Logger.Warnf("[Worker %d] Download %s failed - %s", ctx.WorkerID, fileInfo.SeriesUID, err)
							atomic.AddInt32(&ctx.Stats.Failed, 1)
						} else {
							// Save metadata only for TCIA inputs
							if !isSpreadsheetInput {
								if err := fileInfo.GetMeta(ctx.Options.Output); err != nil {
									Logger.Warnf("[Worker %d] Save meta info %s failed - %s", ctx.WorkerID, fileInfo.SeriesUID, err)
								}
							}
							atomic.AddInt32(&ctx.Stats.Downloaded, 1)
						}
						updateProgress(nil, ctx.Stats, fileInfo.SeriesUID, ctx.Options.Debug, eventsEmit)
					} else {
						Logger.Debugf("[Worker %d] Skip %s (already exists with correct size/checksum)", ctx.WorkerID, fileInfo.SeriesUID)
						atomic.AddInt32(&ctx.Stats.Skipped, 1)
						updateProgress(nil, ctx.Stats, fileInfo.SeriesUID, ctx.Options.Debug, eventsEmit)
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
	updateProgress(nil, stats, "Complete", options.Debug, eventsEmit)

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
		Logger.Warnf("Some downloads failed. Check the logs above for details.")
	}
}
