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
)

var (
	// version and build info
	buildStamp string
	gitHash    string
	goVersion  string
	version    string
	client     *http.Client
	token      *Token
	logger     *zap.SugaredLogger
)

// DownloadStats tracks download statistics
type DownloadStats struct {
	Total          int32
	Downloaded     int32
	Synced         int32
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
	AuthToken  *Token
	Gen3Auth   *Gen3AuthManager
	Options    *Options
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
func decodeInputFile(filePath string, client *http.Client, token *Token, options *Options, s5cmdMap map[string]string) ([]*FileInfo, int, error) {
	ext := strings.ToLower(filepath.Ext(filePath))
	switch ext {
	case ".tcia":
		files, err := decodeTCIA(filePath, client, token, options)
		return files, 0, err
	case ".s5cmd":
		files, newJobs := decodeS5cmd(filePath, options.Output, s5cmdMap)
		return files, newJobs, nil
	case ".csv", ".tsv", ".xlsx":
		// Try to decode as a SeriesInstanceUID spreadsheet first
		seriesUIDs, err := getSeriesUIDsFromSpreadsheet(filePath)
		if err == nil {
			// Success, handle like a TCIA manifest
			files, err := FetchMetadataForSeriesUIDs(seriesUIDs, client, token, options)
			return files, 0, err
		} else if err != ErrSeriesUIDColumnNotFound {
			// A real error occurred
			return nil, 0, fmt.Errorf("could not get series UIDs from spreadsheet: %w", err)
		}

		// Fallback to regular spreadsheet handling
		files, err := decodeSpreadsheet(filePath)
		return files, 0, err
	default:
		return nil, 0, fmt.Errorf("unsupported input file format: %s", ext)
	}
}

// updateProgress prints the current download progress
func updateProgress(stats *DownloadStats, currentSeriesID string) {
	stats.mu.Lock()
	defer stats.mu.Unlock()

	now := time.Now()

	// Update at most once per 200ms for smooth updates
	if now.Sub(stats.LastUpdate) < 200*time.Millisecond {
		return
	}
	stats.LastUpdate = now

	// Calculate progress
	processed := atomic.LoadInt32(&stats.Downloaded) + atomic.LoadInt32(&stats.Synced) + atomic.LoadInt32(&stats.Skipped) + atomic.LoadInt32(&stats.Failed)
	percentage := float64(processed) / float64(stats.Total) * 100

	// Calculate ETA based on download/sync rate
	elapsed := time.Since(stats.StartTime)
	var eta string
	if downloadedAndSynced := atomic.LoadInt32(&stats.Downloaded) + atomic.LoadInt32(&stats.Synced); downloadedAndSynced > 0 && elapsed > 0 {
		rate := float64(downloadedAndSynced) / elapsed.Seconds()
		remainingFiles := float64(stats.Total - processed)
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
	fmt.Fprintf(os.Stderr, "\r\033[K[%d/%d] %.1f%% | Downloaded: %d | Synced: %d | Skipped: %d | Failed: %d%s | Current: %s",
		processed, stats.Total, percentage,
		stats.Downloaded, stats.Synced, stats.Skipped, stats.Failed,
		eta, displayID)
}

func main() {
	setupCloseHandler()

	var options = InitOptions()

	if options.Version {
		logger.Infof("Current version: %s", version)
		logger.Infof("Git Commit Hash: %s", gitHash)
		logger.Infof("UTC Build Time : %s", buildStamp)
		logger.Infof("Golang Version : %s", goVersion)
		os.Exit(0)
	} else {
		client = newClient(options.Proxy, options.MaxConnsPerHost)

		err := os.MkdirAll(options.Output, os.ModePerm)
		if err != nil {
			logger.Fatalf("failed to create output directory: %v", err)
		}
		token, err = NewToken(
			options.Username, options.Password,
			filepath.Join(options.Output, fmt.Sprintf("%s.json", options.Username)))

		if err != nil {
			logger.Fatal(err)
		}

		// Create metadata directory
		if err := createMetadataDir(options.Output); err != nil {
			logger.Fatalf("Failed to create metadata directory: %v", err)
		}

		// Load the s5cmd series map
		s5cmdMap, err := loadS5cmdSeriesMapFromCSVs(options.Output)
		if err != nil {
			logger.Fatalf("Failed to load s5cmd series map from CSVs: %v", err)
		}

		var wg sync.WaitGroup
		files, newS5cmdJobs, err := decodeInputFile(options.Input, client, token, options, s5cmdMap)
		if err != nil {
			logger.Fatalf("Failed to decode input file: %v", err)
		}

		// If input is a spreadsheet, copy it to the metadata folder
		ext := strings.ToLower(filepath.Ext(options.Input))
		if ext == ".csv" || ext == ".tsv" || ext == ".xlsx" {
			metaDir := filepath.Join(options.Output, "metadata")
			destPath := filepath.Join(metaDir, filepath.Base(options.Input))
			if err := copyFile(options.Input, destPath); err != nil {
				logger.Warnf("Failed to copy spreadsheet to metadata folder: %v", err)
			}
		}

		stats := &DownloadStats{Total: int32(len(files))}
		stats.StartTime = time.Now()

		itemType := "items"
		if len(files) > 0 {
			if files[0].S5cmdManifestPath != "" {
				itemType = "series"
			} else if files[0].DRSURI != "" || files[0].DownloadURL != "" {
				itemType = "files"
			}
		}

		if options.Debug {
			logger.Infof("Starting download of %d %s with %d workers", len(files), itemType, options.Concurrent)
		} else {
			fmt.Fprintf(os.Stderr, "\nDownloading %d %s with %d workers...\n\n", len(files), itemType, options.Concurrent)
		}

		wg.Add(options.Concurrent)
		inputChan := make(chan *FileInfo, len(files))

		// Create Gen3 Auth Manager
		gen3Auth, err := NewGen3AuthManager(client, options.Auth)
		if err != nil {
			logger.Fatalf("Failed to initialize Gen3 auth manager: %v", err)
		}

		for i := 0; i < options.Concurrent; i++ {
			ctx := &WorkerContext{
				HTTPClient: client,
				AuthToken:  token,
				Gen3Auth:   gen3Auth,
				Options:    options,
				Stats:      stats,
				WorkerID:   i + 1,
			}

			go func(ctx *WorkerContext, input chan *FileInfo) {
				defer wg.Done()
				for fileInfo := range input {
					updateProgress(ctx.Stats, fileInfo.SeriesUID)
					logger.Debugf("[Worker %d] Processing %s", ctx.WorkerID, fileInfo.SeriesUID)

					isSpreadsheetInput := fileInfo.DownloadURL != "" || fileInfo.DRSURI != "" || fileInfo.S5cmdManifestPath != ""

					if ctx.Options.Meta {
						if isSpreadsheetInput {
							logger.Debugf("[Worker %d] Skipping metadata for item %s", ctx.WorkerID, fileInfo.SeriesUID)
							atomic.AddInt32(&ctx.Stats.Skipped, 1)
						} else {
							if err := fileInfo.GetMeta(ctx.Options.Output); err != nil {
								logger.Warnf("[Worker %d] Save meta info %s failed - %s", ctx.WorkerID, fileInfo.SeriesUID, err)
								atomic.AddInt32(&ctx.Stats.Failed, 1)
							} else {
								atomic.AddInt32(&ctx.Stats.Downloaded, 1)
							}
						}
					} else {
						if ctx.Options.SkipExisting && !fileInfo.NeedsDownload(ctx.Options.Output, false, ctx.Options.NoDecompress) {
							logger.Debugf("[Worker %d] Skip existing %s", ctx.WorkerID, fileInfo.SeriesUID)
							atomic.AddInt32(&ctx.Stats.Skipped, 1)
						} else if fileInfo.NeedsDownload(ctx.Options.Output, ctx.Options.Force, ctx.Options.NoDecompress) {
							if err := fileInfo.Download(ctx.Options.Output, ctx.HTTPClient, ctx.AuthToken, ctx.Gen3Auth, ctx.Options); err != nil {
								logger.Warnf("[Worker %d] Download %s failed - %s", ctx.WorkerID, fileInfo.SeriesUID, err)
								atomic.AddInt32(&ctx.Stats.Failed, 1)
							} else {
								if !isSpreadsheetInput {
									if err := fileInfo.GetMeta(ctx.Options.Output); err != nil {
										logger.Warnf("[Worker %d] Save meta info %s failed - %s", ctx.WorkerID, fileInfo.SeriesUID, err)
									}
								}
								// Increment correct counter
								if fileInfo.IsSyncJob {
									atomic.AddInt32(&ctx.Stats.Synced, 1)
								} else {
									atomic.AddInt32(&ctx.Stats.Downloaded, 1)
								}
							}
						} else {
							logger.Debugf("[Worker %d] Skip %s (already exists with correct size/checksum)", ctx.WorkerID, fileInfo.SeriesUID)
							atomic.AddInt32(&ctx.Stats.Skipped, 1)
						}
					}
					updateProgress(ctx.Stats, fileInfo.SeriesUID)
				}
			}(ctx, inputChan)
		}

		for _, f := range files {
			inputChan <- f
		}
		close(inputChan)
		wg.Wait()

		// Post-processing for s5cmd series
		if newS5cmdJobs > 0 {
			fmt.Println("\nOrganizing s5cmd downloaded series...")
			s5cmdSeriesToFetchMeta := make(map[string]string) // Map SeriesUID to OriginalS5cmdURI

			for _, seriesInfo := range files {
				if seriesInfo.IsSyncJob || seriesInfo.S5cmdManifestPath == "" {
					continue
				}

				tempDir := seriesInfo.S5cmdManifestPath
				filesInDir, err := os.ReadDir(tempDir)
				if err != nil {
					logger.Warnf("Could not read temp directory %s: %v", tempDir, err)
					continue
				}
				if len(filesInDir) == 0 {
					logger.Warnf("No files found in temp directory %s", tempDir)
					os.Remove(tempDir)
					continue
				}

				firstFilePath := filepath.Join(tempDir, filesInDir[0].Name())
				firstDicom, err := ProcessDicomFile(firstFilePath)
				if err != nil {
					logger.Warnf("Could not get SeriesUID from %s: %v", firstFilePath, err)
					continue
				}

				seriesUID := firstDicom.SeriesUID
				finalDir := filepath.Join(options.Output, seriesUID)

				// If the destination directory already exists, remove it. This handles cases
				// where a user manually deletes a metadata entry to re-download a series.
				if _, err := os.Stat(finalDir); err == nil {
					logger.Warnf("Destination directory %s already exists. Removing it before proceeding.", finalDir)
					if err := os.RemoveAll(finalDir); err != nil {
						logger.Errorf("Failed to remove existing directory %s: %v", finalDir, err)
						continue // Skip this series if cleanup fails
					}
				}

				if err := os.Rename(tempDir, finalDir); err != nil {
					logger.Errorf("Could not rename temp dir %s to %s: %v", tempDir, finalDir, err)
					continue
				}
				s5cmdSeriesToFetchMeta[seriesUID] = seriesInfo.OriginalS5cmdURI
			}
			fmt.Println("s5cmd series organization complete.")

			// Fetch and save metadata
			if len(s5cmdSeriesToFetchMeta) > 0 {
				uids := make([]string, 0, len(s5cmdSeriesToFetchMeta))
				for uid := range s5cmdSeriesToFetchMeta {
					uids = append(uids, uid)
				}

				fmt.Println("\nFetching metadata for new s5cmd series...")
				fetchedMetadata, err := FetchMetadataForSeriesUIDs(uids, client, token, options)
				if err != nil {
					logger.Errorf("Failed to fetch s5cmd metadata: %v", err)
				} else {
					for _, meta := range fetchedMetadata {
						meta.OriginalS5cmdURI = s5cmdSeriesToFetchMeta[meta.SeriesUID]
					}
					manifestName := strings.TrimSuffix(filepath.Base(options.Input), filepath.Ext(options.Input))
					csvPath := filepath.Join(options.Output, "metadata", fmt.Sprintf("%s-metadata.csv", manifestName))
					if err := writeMetadataToCSV(csvPath, fetchedMetadata); err != nil {
						logger.Errorf("Failed to write s5cmd metadata to CSV: %v", err)
					} else {
						fmt.Printf("Metadata for %d series saved to %s\n", len(fetchedMetadata), csvPath)
					}
				}
			}
		}

		updateProgress(stats, "Complete")

		if !options.Debug {
			fmt.Fprintf(os.Stderr, "\n")
		}

		elapsed := time.Since(stats.StartTime)
		fmt.Println("\n=== Download Summary ===")
		fmt.Printf("Total items: %d\n", stats.Total)
		fmt.Printf("Downloaded: %d\n", stats.Downloaded)
		if stats.Synced > 0 {
			fmt.Printf("Synced: %d\n", stats.Synced)
		}
		fmt.Printf("Skipped: %d\n", stats.Skipped)
		fmt.Printf("Failed: %d\n", stats.Failed)
		fmt.Printf("Total time: %s\n", elapsed.Round(time.Second))

		if stats.Total > 0 {
			rate := float64(stats.Downloaded+stats.Synced+stats.Skipped) / elapsed.Seconds()
			fmt.Printf("Average rate: %.1f items/second\n", rate)
		}

		if stats.Failed > 0 {
			logger.Warnf("Some downloads failed. Check the logs above for details.")
		}
	}
}
