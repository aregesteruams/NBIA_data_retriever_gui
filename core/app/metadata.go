package app 

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// Defaults (match CLI defaults)
const (
	MetaUrlDefault = "https://services.cancerimagingarchive.net/nbia-api/services/v2/getSeriesMetaData"
)

// MetadataStats tracks metadata fetching progress
type MetadataStats struct {
	Total         int
	Fetched       int32
	Cached        int32
	Failed        int32
	StartTime     time.Time
	LastUpdate    time.Time
	CurrentSeries string
	mu            sync.Mutex
}

func (m *MetadataStats) updateProgress(logFn func(string), action string, seriesID string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.CurrentSeries = seriesID
	switch action {
	case "fetched":
		m.Fetched++
	case "cached":
		m.Cached++
	case "failed":
		m.Failed++
	}

	completed := int(m.Fetched + m.Cached + m.Failed)
	now := time.Now()
	if now.Sub(m.LastUpdate) < 100*time.Millisecond && completed != m.Total {
		return
	}
	m.LastUpdate = now

	if m.Total > 0 {
		percentage := float64(completed) * 100.0 / float64(m.Total)

		// ETA based on fetch rate
		elapsed := time.Since(m.StartTime)
		var eta string
		if m.Fetched > 0 && elapsed > 0 {
			rate := float64(m.Fetched) / elapsed.Seconds()
			remaining := float64(m.Total - int(m.Cached) - int(m.Fetched) - int(m.Failed))
			if remaining > 0 && rate > 0 {
				rem := remaining / rate
				eta = fmt.Sprintf(" | ETA: %s", time.Duration(rem*float64(time.Second)).Round(time.Second))
			}
		}

		displayID := m.CurrentSeries
		if len(displayID) > 30 {
			displayID = displayID[:30] + "..."
		}

		// Emit a simple progress line via logFn
		if logFn != nil {
			logFn(fmt.Sprintf("[%d/%d] %.1f%% | Fetched: %d | Cached: %d | Failed: %d%s | Current: %s",
				completed, m.Total, percentage, m.Fetched, m.Cached, m.Failed, eta, displayID))
		}
	}
}

// FileInfo mirrors the CLI's FileInfo structure used for metadata
type FileInfo struct {
	NumberOfImages     string `json:"Number of Images"`
	SOPClassUID        string `json:"SOP Class UID"`
	Manufacturer       string `json:"Manufacturer"`
	DataDescriptionURI string `json:"Data Description URI"`
	LicenseURL         string `json:"License URL"`
	AnnotationSize     string `json:"Annotation Size"`
	Collection         string `json:"Collection"`
	StudyDescription   string `json:"Study Description"`
	SeriesUID          string `json:"Series UID"`
	StudyUID           string `json:"Study UID"`
	LicenseName        string `json:"License Name"`
	StudyDate          string `json:"Study Date"`
	SeriesDescription  string `json:"Series Description"`
	Modality           string `json:"Modality"`
	RdPartyAnalysis    string `json:"3rd Party Analysis"`
	FileSize           string `json:"File Size"`
	SubjectID          string `json:"Subject ID"`
	SeriesNumber       string `json:"Series Number"`
	MD5Hash            string `json:"MD5 Hash,omitempty"`
	DownloadURL        string `json:"downloadUrl,omitempty"`
}

// getMetadataCachePath returns the path for cached metadata under output/metadata
func getMetadataCachePath(output, seriesUID string) string {
	return filepath.Join(output, "metadata", fmt.Sprintf("%s.json", seriesUID))
}

// createMetadataDir ensures the metadata directory exists
func createMetadataDir(output string) error {
	metaDir := filepath.Join(output, "metadata")
	if _, err := os.Stat(metaDir); os.IsNotExist(err) {
		return os.MkdirAll(metaDir, 0755)
	}
	return nil
}

// loadMetadataFromCache loads cached metadata file
func loadMetadataFromCache(cachePath string) (*FileInfo, error) {
	data, err := os.ReadFile(cachePath)
	if err != nil {
		return nil, err
	}
	var info FileInfo
	if err := json.Unmarshal(data, &info); err != nil {
		return nil, err
	}
	return &info, nil
}

// saveMetadataToCache saves metadata to a cache file atomically
func saveMetadataToCache(info *FileInfo, cachePath string) error {
	// Ensure dir exists
	dir := filepath.Dir(cachePath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}
	tmp := cachePath + ".tmp"
	data, err := json.MarshalIndent(info, "", "\t")
	if err != nil {
		return err
	}
	if err := os.WriteFile(tmp, data, 0644); err != nil {
		return err
	}
	return os.Rename(tmp, cachePath)
}

// makeURL builds the metadata query URL
func makeMetaURL(base string, seriesID string) (string, error) {
	if base == "" {
		base = MetaUrlDefault
	}
	// append query param SeriesInstanceUID
	if strings.Contains(base, "?") {
		return base + "&SeriesInstanceUID=" + seriesID, nil
	}
	return base + "?SeriesInstanceUID=" + seriesID, nil
}

// FetchMetadata reads the manifest at manifestPath, fetches metadata for each series
// using httpClient and getAccessToken to retrieve tokens. It writes cache files to
// output/metadata and streams progress/logs via logFn.
func FetchMetadata(ctx context.Context, manifestPath, output string, httpClient *http.Client, getAccessToken func() (string, error), opts RunOptions, logFn func(string)) ([]*FileInfo, error) {
	ids, err := ParseManifest(manifestPath)
	if err != nil {
		return nil, err
	}

	if err := createMetadataDir(output); err != nil {
		return nil, err
	}

	metaStats := &MetadataStats{Total: len(ids), StartTime: time.Now()}

	workers := opts.MetadataWorkers
	if workers <= 0 {
		workers = 10
	}

	idChan := make(chan string, len(ids))
	for _, id := range ids {
		idChan <- id
	}
	close(idChan)

	var wg sync.WaitGroup
	var mu sync.Mutex
	results := make([]*FileInfo, 0)

	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func(workerID int) {
			defer wg.Done()
			for seriesID := range idChan {
				select {
				case <-ctx.Done():
					return
				default:
				}

				cachePath := getMetadataCachePath(output, seriesID)
				if !opts.RefreshMetadata {
					if cached, err := loadMetadataFromCache(cachePath); err == nil {
						mu.Lock()
						results = append(results, cached)
						mu.Unlock()
						metaStats.updateProgress(logFn, "cached", seriesID)
						continue
					}
				}

				url_, err := makeMetaURL(opts.MetaUrl, seriesID)
				if err != nil {
					if logFn != nil {
						logFn(fmt.Sprintf("[Meta Worker %d] Failed to build URL: %v", workerID, err))
					}
					metaStats.updateProgress(logFn, "failed", seriesID)
					continue
				}

				req, err := http.NewRequestWithContext(ctx, "GET", url_, nil)
				if err != nil {
					if logFn != nil {
						logFn(fmt.Sprintf("[Meta Worker %d] Failed to create request: %v", workerID, err))
					}
					metaStats.updateProgress(logFn, "failed", seriesID)
					continue
				}

				// Get token
				accessToken := ""
				if getAccessToken != nil {
					tkn, err := getAccessToken()
					if err != nil {
						if logFn != nil {
							logFn(fmt.Sprintf("[Meta Worker %d] Failed to get token: %v", workerID, err))
						}
						metaStats.updateProgress(logFn, "failed", seriesID)
						continue
					}
					accessToken = tkn
				}
				if accessToken != "" {
					req.Header.Add("Authorization", "Bearer "+accessToken)
				}

				resp, err := httpClient.Do(req)
				if err != nil {
					if logFn != nil {
						logFn(fmt.Sprintf("[Meta Worker %d] Request error: %v", workerID, err))
					}
					metaStats.updateProgress(logFn, "failed", seriesID)
					continue
				}

				content, err := io.ReadAll(resp.Body)
				_ = resp.Body.Close()
				if err != nil {
					if logFn != nil {
						logFn(fmt.Sprintf("[Meta Worker %d] Read error: %v", workerID, err))
					}
					metaStats.updateProgress(logFn, "failed", seriesID)
					continue
				}

				files := make([]*FileInfo, 0)
				if err := json.Unmarshal(content, &files); err != nil {
					if logFn != nil {
						// Also try to emit the raw response for debugging
						logFn(fmt.Sprintf("[Meta Worker %d] Failed to parse JSON for %s: %v", workerID, seriesID, err))
						// Emit a truncated content preview
						preview := string(content)
						if len(preview) > 200 {
							preview = preview[:200] + "..."
						}
						logFn(preview)
					}
					metaStats.updateProgress(logFn, "failed", seriesID)
					continue
				}

				for _, f := range files {
					if f.SeriesUID != "" {
						if err := saveMetadataToCache(f, getMetadataCachePath(output, f.SeriesUID)); err != nil {
							if logFn != nil {
								logFn(fmt.Sprintf("[Meta Worker %d] Failed to cache %s: %v", workerID, f.SeriesUID, err))
							}
						}
					}
				}

				mu.Lock()
				results = append(results, files...)
				mu.Unlock()

				metaStats.updateProgress(logFn, "fetched", seriesID)
			}
		}(i + 1)
	}

	wg.Wait()

	if logFn != nil {
		logFn(fmt.Sprintf("Successfully fetched metadata for %d files", len(results)))
	}

	return results, nil
}
