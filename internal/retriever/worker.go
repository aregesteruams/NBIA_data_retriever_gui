package retriever

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// Downloadable is an adapter interface for items that can be downloaded or have metadata saved.
type Downloadable interface {
	// NeedsDownload returns true if the item needs to be downloaded (based on adapter's options)
	NeedsDownload(output string) bool
	// Download performs the download and returns an error if it fails. The ctx
	// is honored to allow cancellation of in-progress downloads.
	Download(ctx context.Context, output string) error
	// GetMeta saves metadata for the item (used in meta-only mode)
	GetMeta(output string) error
	// SeriesID returns an identifier used for progress logging
	SeriesID() string
}

// DownloadSummary contains aggregated results of the download run.
type DownloadSummary struct {
	Total      int
	Downloaded int32
	Skipped    int32
	Failed     int32
	Elapsed    time.Duration
}

// OrchestrateDownloads runs a worker pool to process the provided items. If metaOnly is true,
// workers call GetMeta on each item; otherwise they download files. Logs and progress lines
// are emitted via logFn.
func OrchestrateDownloads(ctx context.Context, items []Downloadable, output string, opts RunOptions, metaOnly bool, logFn func(string)) (*DownloadSummary, error) {
	total := len(items)
	if logFn != nil {
		logFn(fmt.Sprintf("Starting %d workers for %d items", opts.Processes, total))
	}

	var downloaded int32
	var skipped int32
	var failed int32

	start := time.Now()

	in := make(chan Downloadable, total)
	for _, it := range items {
		in <- it
	}
	close(in)

	var wg sync.WaitGroup
	workers := opts.Processes
	if workers <= 0 {
		workers = 1
	}
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func(id int) {
			defer wg.Done()
			for item := range in {
				select {
				case <-ctx.Done():
					return
				default:
				}

				sid := item.SeriesID()

				if metaOnly {
					if logFn != nil {
						logFn(fmt.Sprintf("[Worker %d] Saving metadata for %s", id+1, sid))
					}
					if err := item.GetMeta(output); err != nil {
						atomic.AddInt32(&failed, 1)
						if logFn != nil {
							logFn(fmt.Sprintf("[Worker %d] Save meta failed for %s: %v", id+1, sid, err))
						}
					} else {
						atomic.AddInt32(&downloaded, 1)
					}
					continue
				}

				// Normal download flow
				if opts.SkipExisting && !item.NeedsDownload(output) {
					atomic.AddInt32(&skipped, 1)
					if logFn != nil {
						logFn(fmt.Sprintf("[Worker %d] Skip existing %s", id+1, sid))
					}
					continue
				}

				if item.NeedsDownload(output) {
					if logFn != nil {
						logFn(fmt.Sprintf("[Worker %d] Downloading %s", id+1, sid))
					}
					if err := item.Download(ctx, output); err != nil {
						atomic.AddInt32(&failed, 1)
						if logFn != nil {
							logFn(fmt.Sprintf("[Worker %d] Download failed for %s: %v", id+1, sid, err))
						}
					} else {
						atomic.AddInt32(&downloaded, 1)
					}
				} else {
					atomic.AddInt32(&skipped, 1)
					if logFn != nil {
						logFn(fmt.Sprintf("[Worker %d] Skip %s (already exists)", id+1, sid))
					}
				}
			}
		}(i)
	}

	wg.Wait()

	elapsed := time.Since(start)
	summary := &DownloadSummary{
		Total:      total,
		Downloaded: downloaded,
		Skipped:    skipped,
		Failed:     failed,
		Elapsed:    elapsed,
	}

	if logFn != nil {
		logFn(fmt.Sprintf("Completed: downloaded=%d skipped=%d failed=%d elapsed=%s", downloaded, skipped, failed, elapsed.Round(time.Second)))
	}

	return summary, nil
}
