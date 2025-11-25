package main

import (
	"archive/zip"
	"bufio"
	"bytes"
	"context"
	"crypto/md5"
	"encoding/csv"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
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

// updateMetadataProgress updates and displays metadata fetching progress
func (m *MetadataStats) updateProgress(action string, seriesID string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Update current series
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

	// Update display at most once per 100ms or when complete
	if now.Sub(m.LastUpdate) < 100*time.Millisecond && completed != m.Total {
		return
	}
	m.LastUpdate = now

	if m.Total > 0 {
		percentage := float64(completed) * 100.0 / float64(m.Total)

		// Calculate ETA based on fetch rate
		elapsed := time.Since(m.StartTime)
		var eta string
		if m.Fetched > 0 && elapsed > 0 {
			rate := float64(m.Fetched) / elapsed.Seconds()
			remainingToFetch := float64(m.Total - int(m.Cached) - int(m.Fetched) - int(m.Failed))
			if remainingToFetch > 0 && rate > 0 {
				remainingTime := remainingToFetch / rate
				etaDuration := time.Duration(remainingTime * float64(time.Second))
				eta = fmt.Sprintf(" | ETA: %s", etaDuration.Round(time.Second))
			}
		}

		// Truncate series ID for display
		displayID := m.CurrentSeries
		if len(displayID) > 30 {
			displayID = displayID[:30] + "..."
		}

		// Clear line and print progress - identical format to download progress
		fmt.Fprintf(os.Stderr, "\r\033[K[%d/%d] %.1f%% | Fetched: %d | Cached: %d | Failed: %d%s | Current: %s",
			completed, m.Total, percentage,
			m.Fetched, m.Cached, m.Failed,
			eta, displayID)

		if completed == m.Total {
			fmt.Fprintf(os.Stderr, "\n")
		}
	}
}

var (
	// Directory creation mutex
	dirMutex sync.Mutex
	// Metadata cache mutex
	metaMutex sync.Mutex
)

// getMetadataCachePath returns the path for cached metadata
func getMetadataCachePath(output, seriesUID string) string {
	return filepath.Join(output, "metadata", fmt.Sprintf("%s.json", seriesUID))
}

// createMetadataDir creates the metadata directory if it doesn't exist
func createMetadataDir(output string) error {
	metaDir := filepath.Join(output, "metadata")
	if _, err := os.Stat(metaDir); os.IsNotExist(err) {
		return os.MkdirAll(metaDir, 0755)
	}
	return nil
}

// loadMetadataFromCache loads metadata from cache file
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

// saveMetadataToCache saves metadata to cache file
func saveMetadataToCache(info *FileInfo, cachePath string) error {
	metaMutex.Lock()
	defer metaMutex.Unlock()

	// Ensure directory exists
	dir := filepath.Dir(cachePath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}

	// Write to temp file first for atomic operation
	tempFile := cachePath + ".tmp"
	data, err := json.MarshalIndent(info, "", "\t")
	if err != nil {
		return err
	}

	if err := os.WriteFile(tempFile, data, 0644); err != nil {
		return err
	}

	// Atomic rename
	return os.Rename(tempFile, cachePath)
}

// FetchMetadataForSeriesUIDs fetches metadata for a list of series UIDs in parallel
func FetchMetadataForSeriesUIDs(seriesIDs []string, httpClient *http.Client, authToken *Token, options *Options) ([]*FileInfo, error) {
	fmt.Printf("Found %d series to fetch metadata for\n", len(seriesIDs))

	// Initialize metadata stats
	metaStats := &MetadataStats{
		Total:     len(seriesIDs),
		StartTime: time.Now(),
	}

	// Use parallel workers to fetch metadata
	metadataWorkers := options.MetadataWorkers
	var wg sync.WaitGroup
	var mu sync.Mutex
	results := make([]*FileInfo, 0)

	// Create a channel for series IDs
	idChan := make(chan string, len(seriesIDs))
	for _, id := range seriesIDs {
		idChan <- id
	}
	close(idChan)

	// Start workers
	wg.Add(metadataWorkers)
	for i := 0; i < metadataWorkers; i++ {
		go func(workerID int) {
			defer wg.Done()

			for seriesID := range idChan {
				// Check cache first unless refresh is requested
				cachePath := getMetadataCachePath(options.Output, seriesID)

				if !options.RefreshMetadata {
					// Try to load from cache
					if cachedInfo, err := loadMetadataFromCache(cachePath); err == nil {
						logger.Debugf("[Meta Worker %d] Loaded metadata from cache for: %s", workerID, seriesID)
						mu.Lock()
						results = append(results, cachedInfo)
						mu.Unlock()
						metaStats.updateProgress("cached", seriesID)
						continue
					}
					// Cache miss or error, fetch from API
					logger.Debugf("[Meta Worker %d] Cache miss, fetching metadata for: %s", workerID, seriesID)
				} else {
					logger.Debugf("[Meta Worker %d] Force refresh, fetching metadata for: %s", workerID, seriesID)
				}

				url_, err := makeURL(MetaUrl, map[string]interface{}{"SeriesInstanceUID": seriesID})
				if err != nil {
					logger.Errorf("[Meta Worker %d] Failed to make URL: %v", workerID, err)
					metaStats.updateProgress("failed", seriesID)
					continue
				}

				req, err := http.NewRequest("GET", url_, nil)
				if err != nil {
					logger.Errorf("[Meta Worker %d] Failed to create request: %v", workerID, err)
					metaStats.updateProgress("failed", seriesID)
					continue
				}

				// Get current access token
				accessToken, err := authToken.GetAccessToken()
				if err != nil {
					logger.Errorf("[Meta Worker %d] Failed to get access token: %v", workerID, err)
					metaStats.updateProgress("failed", seriesID)
					continue
				}
				req.Header.Add("Authorization", fmt.Sprintf("Bearer %s", accessToken))

				// Set timeout for metadata request
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				req = req.WithContext(ctx)

				resp, err := doRequest(httpClient, req)
				cancel() // Cancel context after request
				if err != nil {
					logger.Errorf("[Meta Worker %d] Failed to do request: %v", workerID, err)
					metaStats.updateProgress("failed", seriesID)
					continue
				}

				// Check for authentication errors
				if resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden {
					logger.Errorf("[Meta Worker %d] Authentication failed for series %s (status: %s). Please check your credentials and ensure you have access to this restricted series.", workerID, seriesID, resp.Status)
					_ = resp.Body.Close()
					metaStats.updateProgress("failed", seriesID)
					continue
				}

				content, err := io.ReadAll(resp.Body)
				_ = resp.Body.Close()
				if err != nil {
					logger.Errorf("[Meta Worker %d] Failed to read response data: %v", workerID, err)
					metaStats.updateProgress("failed", seriesID)
					continue
				}

				var files []*FileInfo
				// The API sometimes returns a single object instead of an array for a single series.
				// We need to handle both cases.
				if len(content) > 0 && content[0] == '[' {
					err = json.Unmarshal(content, &files)
				} else if len(content) > 0 {
					var file FileInfo
					err = json.Unmarshal(content, &file)
					if err == nil {
						files = []*FileInfo{&file}
					}
				}

				if err != nil {
					logger.Errorf("[Meta Worker %d] Failed to parse response data: %v", workerID, err)
					logger.Debugf("%s", string(content))
					metaStats.updateProgress("failed", seriesID)
					continue
				}

				// Save to cache - usually one file per series
				for _, file := range files {
					if file.SeriesUID != "" {
						if err := saveMetadataToCache(file, getMetadataCachePath(options.Output, file.SeriesUID)); err != nil {
							logger.Warnf("[Meta Worker %d] Failed to cache metadata for %s: %v", workerID, file.SeriesUID, err)
						}
					}
				}

				// Thread-safe append to results
				mu.Lock()
				results = append(results, files...)
				mu.Unlock()

				// Mark as successfully fetched
				metaStats.updateProgress("fetched", seriesID)
			}
		}(i + 1)
	}

	// Wait for all workers to finish
	wg.Wait()

	fmt.Printf("Successfully fetched metadata for %d files\n", len(results))
	return results, nil
}

// decodeTCIA is used to decode the tcia file with parallel metadata fetching
func decodeTCIA(path string, httpClient *http.Client, authToken *Token, options *Options) ([]*FileInfo, error) {
	logger.Debugf("decoding tcia file: %s", path)

	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	// First, collect all series IDs
	seriesIDs := make([]string, 0)
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()
		if !strings.ContainsAny(line, "=") {
			seriesIDs = append(seriesIDs, line)
		}
	}
	if err := scanner.Err(); err != nil {
		logger.Errorf("error reading tcia file: %v", err)
	}

	return FetchMetadataForSeriesUIDs(seriesIDs, httpClient, authToken, options)
}

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
	DRSURI             string `json:"drs_uri,omitempty"`
	S5cmdManifestPath  string `json:"s5cmd_manifest_path,omitempty"`
	FileName           string `json:"file_name,omitempty"`
	OriginalS5cmdURI   string `json:"original_s5cmd_uri,omitempty"`
	IsSyncJob          bool   `json:"is_sync_job,omitempty"`
}

// GetOutput construct the output directory (thread-safe)
func (info *FileInfo) getOutput(output string) string {
	outputDir := filepath.Join(output, info.SubjectID, info.StudyUID)

	// Check if directory exists without lock first
	if _, err := os.Stat(outputDir); !os.IsNotExist(err) {
		return outputDir
	}

	// Directory doesn't exist, acquire lock to create it
	dirMutex.Lock()
	defer dirMutex.Unlock()

	// Double-check after acquiring lock
	if _, err := os.Stat(outputDir); os.IsNotExist(err) {
		if err = os.MkdirAll(outputDir, 0755); err != nil {
			logger.Fatal(err)
		}
	}

	return outputDir
}

func (info *FileInfo) MetaFile(output string) string {
	return getMetadataCachePath(output, info.SeriesUID)
}

func (info *FileInfo) DcimFiles(output string) string {
	return filepath.Join(info.getOutput(output), info.SeriesUID)
}

// NeedsDownload checks if files need to be downloaded
func (info *FileInfo) NeedsDownload(output string, force bool, noDecompress bool) bool {
	if force {
		logger.Debugf("Force flag set, will re-download %s", info.SeriesUID)
		return true
	}

	var targetPath string
	if info.S5cmdManifestPath != "" {
		// s5cmd downloads files to the output directory, so we can't check for a specific file
		// and we assume the file needs to be downloaded.
		return true
	}
	if info.DownloadURL != "" {
		targetPath = filepath.Join(output, info.SeriesUID)
		_, err := os.Stat(targetPath)
		if os.IsNotExist(err) {
			logger.Debugf("Target %s does not exist, need to download", targetPath)
			return true
		}
		// If it exists, we assume it's downloaded. We don't have size/checksum info for these.
		logger.Debugf("Direct download file %s exists, skipping", targetPath)
		return false
	}

	if noDecompress {
		// Check for ZIP file
		targetPath = info.DcimFiles(output) + ".zip"
	} else {
		// Check for extracted directory
		targetPath = info.DcimFiles(output)
	}

	stat, err := os.Stat(targetPath)
	if err != nil {
		if os.IsNotExist(err) {
			logger.Debugf("Target %s does not exist, need to download", targetPath)
			return true
		}
		logger.Warnf("Error checking target %s: %v", targetPath, err)
		return true
	}

	if noDecompress {
		// For ZIP files, check if it's a regular file
		if stat.IsDir() {
			logger.Debugf("%s exists but is a directory, need to re-download", targetPath)
			return true
		}
		// For ZIP files, we can't easily verify the size as it's compressed
		// Just check existence for now
		logger.Debugf("ZIP file %s exists, skipping", targetPath)
		return false
	} else {
		// For extracted files, check if it's a directory
		if !stat.IsDir() {
			logger.Debugf("%s exists but is not a directory, need to re-download", targetPath)
			return true
		}

		// Check total size of extracted files
		if info.FileSize != "" {
			expectedSize, err := strconv.ParseInt(info.FileSize, 10, 64)
			if err == nil {
				actualSize, err := getDirectorySize(targetPath)
				if err != nil {
					logger.Warnf("Error calculating directory size for %s: %v", targetPath, err)
					return true
				}
				if actualSize != expectedSize {
					logger.Debugf("Directory %s size mismatch: expected %d, got %d", targetPath, expectedSize, actualSize)
					return true
				}
			}
		}

		logger.Debugf("Directory %s exists with correct size, skipping", targetPath)
		return false
	}
}

// extractAndVerifyZip extracts a ZIP file and verifies the total uncompressed size and optional MD5 hashes
func extractAndVerifyZip(zipPath string, destDir string, expectedSize int64, md5Map map[string]string) error {
	reader, err := zip.OpenReader(zipPath)
	if err != nil {
		return fmt.Errorf("failed to open zip: %v", err)
	}
	defer reader.Close()

	// Create destination directory
	if err := os.MkdirAll(destDir, 0755); err != nil {
		return fmt.Errorf("failed to create directory: %v", err)
	}

	var totalSize int64
	var md5Errors []string

	// Check if we're in MD5 validation mode
	md5Mode := len(md5Map) > 0

	// Extract files
	for _, file := range reader.File {
		// Skip md5hashes.csv if present
		if file.Name == "md5hashes.csv" {
			continue
		}

		path := filepath.Join(destDir, file.Name)

		// Ensure the file path is within destDir (security check)
		if !strings.HasPrefix(path, filepath.Clean(destDir)+string(os.PathSeparator)) {
			return fmt.Errorf("invalid file path in zip: %s", file.Name)
		}

		if file.FileInfo().IsDir() {
			if err := os.MkdirAll(path, file.Mode()); err != nil {
				return fmt.Errorf("failed to create directory: %v", err)
			}
			continue
		}

		// Create the directory for the file
		if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
			return fmt.Errorf("failed to create file directory: %v", err)
		}

		// Extract file
		fileReader, err := file.Open()
		if err != nil {
			return fmt.Errorf("failed to open file in zip: %v", err)
		}

		targetFile, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, file.Mode())
		if err != nil {
			fileReader.Close()
			return fmt.Errorf("failed to create file: %v", err)
		}

		// Check if this file is in the MD5 map (i.e., it's an imaging file)
		isImagingFile := false
		expectedMD5 := ""
		if md5Hash, ok := md5Map[file.Name]; ok {
			isImagingFile = true
			expectedMD5 = md5Hash
		}

		// If MD5 validation is needed, use a multi-writer
		var writer io.Writer = targetFile
		var hasher hash.Hash
		if isImagingFile && expectedMD5 != "" {
			hasher = md5.New()
			writer = io.MultiWriter(targetFile, hasher)
		}

		written, err := io.Copy(writer, fileReader)
		fileReader.Close()
		targetFile.Close()

		if err != nil {
			return fmt.Errorf("failed to extract file %s: %v", file.Name, err)
		}

		// Verify MD5 if available
		if hasher != nil && expectedMD5 != "" {
			actualMD5 := hex.EncodeToString(hasher.Sum(nil))
			if actualMD5 != expectedMD5 {
				md5Errors = append(md5Errors, fmt.Sprintf("%s: expected %s, got %s", file.Name, expectedMD5, actualMD5))
			} else {
				logger.Debugf("MD5 verified for %s", file.Name)
			}
		}

		// Only count size for imaging files in MD5 mode, or all files in non-MD5 mode
		if md5Mode {
			if isImagingFile {
				totalSize += written
			}
		} else {
			totalSize += written
		}
	}

	// Report MD5 errors if any
	if len(md5Errors) > 0 {
		return fmt.Errorf("MD5 validation failed for %d files:\n%s", len(md5Errors), strings.Join(md5Errors, "\n"))
	}

	// Verify total size if expected size is provided
	if expectedSize > 0 && totalSize != expectedSize {
		if md5Mode {
			// In MD5 mode, we know exactly which files are imaging files, so size should match
			return fmt.Errorf("size mismatch: expected %d bytes, extracted %d bytes", expectedSize, totalSize)
		} else {
			// In non-MD5 mode, we counted all files including non-imaging files, so just warn
			logger.Warnf("Size mismatch (this may be due to non-imaging files in the archive): expected %d bytes, extracted %d bytes", expectedSize, totalSize)
		}
	}

	return nil
}

// getDirectorySize calculates the total size of all files in a directory
func getDirectorySize(dirPath string) (int64, error) {
	var size int64
	err := filepath.Walk(dirPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			size += info.Size()
		}
		return nil
	})
	return size, err
}

// parseMD5HashesCSV parses the md5hashes.csv file from the ZIP and returns a map of filename to MD5 hash
func parseMD5HashesCSV(zipPath string) (map[string]string, error) {
	reader, err := zip.OpenReader(zipPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open zip: %v", err)
	}
	defer reader.Close()

	// Find md5hashes.csv in the ZIP
	for _, file := range reader.File {
		if file.Name == "md5hashes.csv" {
			rc, err := file.Open()
			if err != nil {
				return nil, fmt.Errorf("failed to open md5hashes.csv: %v", err)
			}
			defer rc.Close()

			// Parse CSV
			csvReader := csv.NewReader(rc)
			records, err := csvReader.ReadAll()
			if err != nil {
				return nil, fmt.Errorf("failed to parse CSV: %v", err)
			}

			// Build map (skip header)
			md5Map := make(map[string]string)
			for i, record := range records {
				if i == 0 || len(record) < 2 {
					continue // Skip header or invalid rows
				}
				filename := record[0]
				md5Hash := record[1]
				md5Map[filename] = md5Hash
			}

			return md5Map, nil
		}
	}

	return nil, fmt.Errorf("md5hashes.csv not found in ZIP")
}

func (info *FileInfo) GetMeta(output string) error {
	logger.Debugf("getting meta information and save to %s", output)
	f, err := os.OpenFile(info.MetaFile(output), os.O_WRONLY|os.O_CREATE|os.O_TRUNC, os.ModePerm)
	if err != nil {
		return fmt.Errorf("failed to open meta file %s: %v", info.MetaFile(output), err)
	}
	content, err := json.MarshalIndent(info, "", "\t")
	if err != nil {
		return fmt.Errorf("failed to marshall meta: %v", err)
	}
	_, err = f.Write(content)
	if err != nil {
		return err
	}
	return f.Close()
}

// Download is real function to download file with retry logic
func (info *FileInfo) Download(output string, httpClient *http.Client, authToken *Token, gen3Auth *Gen3AuthManager, options *Options) error {
	// Add rate limiting delay between requests
	if options.RequestDelay > 0 {
		time.Sleep(options.RequestDelay)
	}
	return info.DownloadWithRetry(output, httpClient, authToken, gen3Auth, options)
}

// DownloadWithRetry downloads file with retry logic and exponential backoff
func (info *FileInfo) DownloadWithRetry(output string, httpClient *http.Client, authToken *Token, gen3Auth *Gen3AuthManager, options *Options) error {
	var lastErr error
	delay := options.RetryDelay

	for attempt := 0; attempt <= options.MaxRetries; attempt++ {
		if attempt > 0 {
			logger.Infof("Retrying download %s (attempt %d/%d) after %v delay", info.SeriesUID, attempt, options.MaxRetries, delay)
			time.Sleep(delay)
			delay *= 2 // Exponential backoff
		}

		err := info.doDownload(output, httpClient, authToken, gen3Auth, options)
		if err == nil {
			return nil
		}

		lastErr = err
		logger.Warnf("Download %s failed (attempt %d/%d): %v", info.SeriesUID, attempt+1, options.MaxRetries+1, err)

		// Check if error is retryable
		if !isRetryableError(err) {
			logger.Errorf("Non-retryable error for %s: %v", info.SeriesUID, err)
			return err
		}
	}

	return fmt.Errorf("download failed after %d attempts: %v", options.MaxRetries+1, lastErr)
}

// isRetryableError checks if an error is retryable
func isRetryableError(err error) bool {
	// Check for network errors, timeouts, and certain HTTP status codes
	errStr := err.Error()

	// s5cmd errors are generally not retryable
	if strings.Contains(errStr, "s5cmd command failed") {
		return false
	}

	return strings.Contains(errStr, "timeout") ||
		strings.Contains(errStr, "connection refused") ||
		strings.Contains(errStr, "connection reset") ||
		strings.Contains(errStr, "EOF") ||
		strings.Contains(errStr, "incomplete download") || // Truncated downloads
		strings.Contains(errStr, "closed") || // Connection closed
		strings.Contains(errStr, "broken pipe") || // Broken connection
		strings.Contains(errStr, "429") || // Rate limiting
		strings.Contains(errStr, "500") || // Server error
		strings.Contains(errStr, "502") || // Bad gateway
		strings.Contains(errStr, "503") || // Service unavailable
		strings.Contains(errStr, "504") // Gateway timeout
}

// doDownload is a dispatcher for different download types
func (info *FileInfo) doDownload(output string, httpClient *http.Client, authToken *Token, gen3Auth *Gen3AuthManager, options *Options) error {
	// For s5cmd manifest downloads, S5cmdManifestPath is set to the temporary series directory
	if info.S5cmdManifestPath != "" {
		return info.downloadFromS3(info.S5cmdManifestPath, options)
	}
	if strings.HasPrefix(info.DownloadURL, "s3://") {
		// This handles other potential S3 downloads that are not from a manifest
		return info.downloadFromS3(output, options)
	}
	if info.DRSURI != "" {
		return info.downloadFromGen3(output, httpClient, gen3Auth, options)
	}
	if info.DownloadURL != "" {
		return info.downloadDirect(output, httpClient)
	}
	return info.downloadFromTCIA(output, httpClient, authToken, options)
}

// downloadFromS3 downloads a file (or files, using a wildcard) from S3 using the s5cmd command-line tool.
func (info *FileInfo) downloadFromS3(targetDir string, options *Options) error {
	// Ensure the target directory exists, especially for sync jobs where the dir might have been deleted.
	if err := os.MkdirAll(targetDir, 0755); err != nil {
		return fmt.Errorf("could not create target directory %s: %w", targetDir, err)
	}

	var cmd *exec.Cmd
	if info.IsSyncJob {
		logger.Debugf("Syncing from S3: %s to %s", info.DownloadURL, targetDir)
		cmd = exec.Command("s5cmd",
			"--no-sign-request",
			"--endpoint-url", "https://s3.amazonaws.com",
			"sync",
			"--size-only",
			info.DownloadURL,
			".",
		)
	} else {
		logger.Debugf("Copying from S3: %s to %s", info.DownloadURL, targetDir)
		cmd = exec.Command("s5cmd",
			"--no-sign-request",
			"--endpoint-url", "https://s3.amazonaws.com",
			"cp",
			info.DownloadURL,
			".",
		)
	}

	cmd.Dir = targetDir // Run the command in the specified target directory

	// Execute the command
	stdout, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("s5cmd command failed for %s: %s\nOutput: %s", info.DownloadURL, err, string(stdout))
	}

	logger.Debugf("s5cmd output for %s:\n%s", info.DownloadURL, string(stdout))
	return nil
}

// downloadFromGen3 downloads a file from a Gen3 server
func (info *FileInfo) downloadFromGen3(output string, httpClient *http.Client, gen3Auth *Gen3AuthManager, options *Options) error {
	logger.Debugf("Downloading from Gen3 DRS URI: %s", info.DRSURI)

	// Parse DRS URI
	parsedURI, err := url.Parse(info.DRSURI)
	if err != nil {
		return fmt.Errorf("invalid DRS URI: %s", info.DRSURI)
	}
	commonsURL := parsedURI.Host
	objectID := strings.TrimPrefix(parsedURI.Path, "/")

	// Get download URL from Gen3
	objectID = url.PathEscape(objectID)
	downloadURL, err := getGen3DownloadURL(httpClient, commonsURL, objectID, gen3Auth)
	if err != nil {
		return fmt.Errorf("failed to get download URL from Gen3: %v", err)
	}

	// Download the file
	info.DownloadURL = downloadURL
	return info.downloadDirect(output, httpClient)
}

type AccessMethod struct {
	AccessID string `json:"access_id"`
	Type     string `json:"type"`
}

// Gen3AuthManager handles fetching and caching of Gen3 access tokens.
type Gen3AuthManager struct {
	client *http.Client
	apiKey string
	tokens map[string]string // Cache: host -> access token
	mu     sync.Mutex
}

// NewGen3AuthManager creates a new Gen3AuthManager.
func NewGen3AuthManager(client *http.Client, authFile string) (*Gen3AuthManager, error) {
	if authFile == "" {
		// No auth file provided, return a manager that can't authenticate.
		return &Gen3AuthManager{}, nil
	}

	keyData, err := os.ReadFile(authFile)
	if err != nil {
		return nil, fmt.Errorf("failed to read API key file: %v", err)
	}

	var apiKeyData struct {
		APIKey string `json:"api_key"`
	}
	if err := json.Unmarshal(keyData, &apiKeyData); err != nil {
		return nil, fmt.Errorf("failed to parse API key from JSON: %v", err)
	}

	if apiKeyData.APIKey == "" {
		return nil, fmt.Errorf("'api_key' not found in JSON key file")
	}

	return &Gen3AuthManager{
		client: client,
		apiKey: strings.TrimSpace(apiKeyData.APIKey),
		tokens: make(map[string]string),
	}, nil
}

// GetAccessToken retrieves a token for a given Gen3 host, using the cache if possible.
func (m *Gen3AuthManager) GetAccessToken(commonsURL string) (string, error) {
	if m.apiKey == "" {
		return "", fmt.Errorf("Gen3 authentication requires an API key, but none was provided")
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	// Check cache first
	if token, found := m.tokens[commonsURL]; found {
		logger.Debugf("Using cached Gen3 access token for %s", commonsURL)
		return token, nil
	}

	// Not in cache, fetch new token
	logger.Infof("Fetching new Gen3 access token for %s", commonsURL)
	token, err := getGen3AccessToken(m.client, commonsURL, m.apiKey)
	if err != nil {
		return "", err
	}

	// Store in cache
	m.tokens[commonsURL] = token
	return token, nil
}

// getGen3AccessToken retrieves an access token from Gen3 using an API key
func getGen3AccessToken(client *http.Client, commonsURL, apiKey string) (string, error) {
	apiEndpoint := fmt.Sprintf("https://%s/user/credentials/api/access_token", commonsURL)
	apiKeyJSON, err := json.Marshal(map[string]string{"api_key": apiKey})
	if err != nil {
		return "", fmt.Errorf("failed to marshal API key: %v", err)
	}

	req, err := http.NewRequest("POST", apiEndpoint, bytes.NewBuffer(apiKeyJSON))
	if err != nil {
		return "", fmt.Errorf("failed to create request for access token: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("failed to make request for access token: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("Gen3 access token endpoint returned status %s", resp.Status)
	}

	var result map[string]string
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", fmt.Errorf("failed to decode access token response: %v", err)
	}

	accessToken, ok := result["access_token"]
	if !ok {
		return "", fmt.Errorf("no 'access_token' found in Gen3 response")
	}

	logger.Infof("Successfully retrieved Gen3 access token: %s", accessToken)
	return accessToken, nil
}

// getGen3DownloadURL retrieves the download URL from a Gen3 server
func getGen3DownloadURL(client *http.Client, commonsURL, objectID string, gen3Auth *Gen3AuthManager) (string, error) {
	apiEndpoint := fmt.Sprintf("https://%s/user/data/download/%s", commonsURL, objectID)

	req, err := http.NewRequest("GET", apiEndpoint, nil)
	if err != nil {
		return "", fmt.Errorf("failed to create request: %v", err)
	}

	// If a manager is configured and has an API key, get and use a token.
	if gen3Auth != nil && gen3Auth.apiKey != "" {
		accessToken, err := gen3Auth.GetAccessToken(commonsURL)
		if err != nil {
			return "", fmt.Errorf("failed to get access token for %s: %v", commonsURL, err)
		}
		req.Header.Add("Authorization", fmt.Sprintf("Bearer %s", accessToken))
	}

	// Log the request for debugging
	logger.Warnf("Gen3 API Request URL: %s", req.URL.String())
	for name, headers := range req.Header {
		for _, h := range headers {
			logger.Warnf("Gen3 API Request Header: %s: %s", name, h)
		}
	}

	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("failed to make request to Gen3 API: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("Gen3 API returned status %s", resp.Status)
	}

	var result map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		// It's possible the response is just the URL, not a JSON object.
		// To handle this, we can try to read the response body as a string.
		bodyBytes, readErr := io.ReadAll(resp.Body)
		if readErr != nil {
			return "", fmt.Errorf("failed to decode and read Gen3 API response: %v and %v", err, readErr)
		}
		return string(bodyBytes), nil
	}

	accessURL, ok := result["url"].(string)
	if !ok {
		return "", fmt.Errorf("no 'url' found in Gen3 API response")
	}

	return accessURL, nil
}

// downloadDirect downloads a file from a direct URL without decompression
func (info *FileInfo) downloadDirect(output string, httpClient *http.Client) error {
	logger.Debugf("Downloading direct from URL: %s", info.DownloadURL)

	fileName := info.SeriesUID
	if info.FileName != "" {
		fileName = info.FileName
	}
	finalPath := filepath.Join(output, fileName)
	tempPath := finalPath + ".tmp"

	// Clean up any previous temporary files
	if _, err := os.Stat(tempPath); err == nil {
		logger.Debugf("Removing incomplete download: %s", tempPath)
		os.Remove(tempPath)
	}

	req, err := http.NewRequest("GET", info.DownloadURL, nil)
	if err != nil {
		return fmt.Errorf("failed to create request: %v", err)
	}

	// Use a reasonable timeout for direct downloads
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()
	req = req.WithContext(ctx)

	resp, err := doRequest(httpClient, req)
	if err != nil {
		return fmt.Errorf("failed to do request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("HTTP error %d: %s", resp.StatusCode, resp.Status)
	}

	f, err := os.OpenFile(tempPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("failed to open file: %v", err)
	}
	defer func() {
		f.Close()
		if err != nil {
			os.Remove(tempPath)
		}
	}()

	written, err := io.Copy(f, resp.Body)
	if err != nil {
		return fmt.Errorf("failed to write data after %d bytes: %v", written, err)
	}

	logger.Debugf("Downloaded %d bytes for %s", written, info.SeriesUID)

	if err := f.Close(); err != nil {
		return fmt.Errorf("failed to close file: %v", err)
	}

	// Atomic rename to final location
	if err := os.Rename(tempPath, finalPath); err != nil {
		return fmt.Errorf("failed to move file: %v", err)
	}

	logger.Debugf("Successfully saved %s as %s", info.SeriesUID, finalPath)
	return nil
}

// downloadFromTCIA performs the actual download from TCIA, with decompression
func (info *FileInfo) downloadFromTCIA(output string, httpClient *http.Client, authToken *Token, options *Options) error {
	logger.Debugf("getting image file to %s", output)

	url_, err := makeURL(ImageUrl, map[string]interface{}{"SeriesInstanceUID": info.SeriesUID})
	if err != nil {
		return fmt.Errorf("failed to make URL: %v", err)
	}

	// Paths based on decompression mode
	var finalPath string
	var tempZipPath string

	if options.NoDecompress {
		// Keep as ZIP file
		finalPath = info.DcimFiles(output) + ".zip"
		tempZipPath = finalPath + ".tmp"
	} else {
		// Extract to directory
		finalPath = info.DcimFiles(output)
		tempZipPath = finalPath + ".zip.tmp"
	}

	// Clean up any previous temporary files
	if _, err := os.Stat(tempZipPath); err == nil {
		logger.Debugf("Removing incomplete download: %s", tempZipPath)
		os.Remove(tempZipPath)
	}

	// For extraction mode, also clean up temporary extraction directory
	if !options.NoDecompress {
		tempExtractDir := finalPath + ".uncompressed.tmp"
		if _, err := os.Stat(tempExtractDir); err == nil {
			logger.Debugf("Removing incomplete extraction: %s", tempExtractDir)
			os.RemoveAll(tempExtractDir)
		}
	}

	req, err := http.NewRequest("GET", url_, nil)
	if err != nil {
		return fmt.Errorf("failed to create request: %v", err)
	}

	// Get current access token
	accessToken, err := authToken.GetAccessToken()
	if err != nil {
		return fmt.Errorf("failed to get access token: %v", err)
	}
	req.Header.Add("Authorization", fmt.Sprintf("Bearer %s", accessToken))

	// Set timeout based on file size (if known)
	var timeout time.Duration
	if info.FileSize != "" {
		fileSize, _ := strconv.ParseInt(info.FileSize, 10, 64)
		// Calculate timeout: base 5 minutes + 1 minute per 100MB
		timeout = 5*time.Minute + time.Duration(fileSize/(100*1024*1024))*time.Minute
		// Cap at 60 minutes for very large files
		if timeout > 60*time.Minute {
			timeout = 60 * time.Minute
		}
	} else {
		// Default timeout for unknown size
		timeout = 30 * time.Minute
	}
	logger.Debugf("Setting download timeout to %v for %s", timeout, info.SeriesUID)
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	req = req.WithContext(ctx)

	resp, err := doRequest(httpClient, req)
	if err != nil {
		return fmt.Errorf("failed to do request: %v", err)
	}
	defer resp.Body.Close()

	// Log response headers for debugging
	logger.Debugf("Response headers for %s: Status=%s, Content-Length=%d, Transfer-Encoding=%s",
		info.SeriesUID, resp.Status, resp.ContentLength, resp.Header.Get("Transfer-Encoding"))

	// Check HTTP status
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("HTTP error %d: %s", resp.StatusCode, resp.Status)
	}

	// Create new temp ZIP file
	f, err := os.OpenFile(tempZipPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("failed to open file: %v", err)
	}
	defer func() {
		f.Close()
		// Clean up temp files on error
		if err != nil {
			os.Remove(tempZipPath)
			if !options.NoDecompress {
				tempExtractDir := finalPath + ".uncompressed.tmp"
				os.RemoveAll(tempExtractDir)
			}
		}
	}()

	// Log download start
	if resp.ContentLength > 0 {
		logger.Debugf("Downloading %s (size: %d bytes)", info.SeriesUID, resp.ContentLength)
	} else {
		logger.Debugf("Downloading %s (size: unknown)", info.SeriesUID)
	}

	// Buffer the response body for better handling of chunked transfers
	bufferedReader := bufio.NewReaderSize(resp.Body, 64*1024) // 64KB buffer

	// Download without progress bar
	written, err := io.Copy(f, bufferedReader)
	if err != nil {
		// Log detailed error information
		logger.Errorf("Download error for %s: %v (written=%d bytes)", info.SeriesUID, err, written)
		// Check if it's an EOF error (connection closed)
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			logger.Errorf("Connection closed prematurely by server for %s", info.SeriesUID)
		}
		return fmt.Errorf("failed to write data after %d bytes: %v", written, err)
	}

	logger.Debugf("Downloaded %d bytes for %s", written, info.SeriesUID)

	// Note: FileSize in manifest is the uncompressed size, but we download ZIP files
	// So we cannot validate the downloaded size against FileSize
	// Log the download completion instead
	if info.FileSize != "" {
		expectedSize, _ := strconv.ParseInt(info.FileSize, 10, 64)
		compressionRatio := float64(written) / float64(expectedSize) * 100
		logger.Debugf("Downloaded %s: %d bytes (%.1f%% of uncompressed size %d)",
			info.SeriesUID, written, compressionRatio, expectedSize)
	}

	// Close ZIP file before extraction
	if err := f.Close(); err != nil {
		return fmt.Errorf("failed to close file: %v", err)
	}

	if options.NoDecompress {
		// No decompression mode: just move the ZIP file to final location

		// Remove any existing file
		if _, err := os.Stat(finalPath); err == nil {
			logger.Debugf("Removing existing file: %s", finalPath)
			if err := os.Remove(finalPath); err != nil {
				return fmt.Errorf("failed to remove existing file: %v", err)
			}
		}

		// Atomic rename from temp to final location
		if err := os.Rename(tempZipPath, finalPath); err != nil {
			return fmt.Errorf("failed to move ZIP file: %v", err)
		}

		logger.Debugf("Successfully saved %s as %s", info.SeriesUID, finalPath)
		return nil
	} else {
		// Decompression mode: extract and verify
		tempExtractDir := finalPath + ".uncompressed.tmp"

		// Extract and verify the ZIP file
		expectedSize := int64(0)
		if info.FileSize != "" {
			expectedSize, _ = strconv.ParseInt(info.FileSize, 10, 64)
		}

		// Parse MD5 hashes if MD5 validation is enabled (default)
		var md5Map map[string]string
		if !options.NoMD5 {
			md5Map, err = parseMD5HashesCSV(tempZipPath)
			if err != nil {
				logger.Warnf("Failed to parse MD5 hashes: %v", err)
				// Continue without MD5 validation
				md5Map = nil
			}
		}

		logger.Debugf("Extracting %s to %s", tempZipPath, tempExtractDir)
		if err := extractAndVerifyZip(tempZipPath, tempExtractDir, expectedSize, md5Map); err != nil {
			// Clean up temp files on extraction failure
			logger.Errorf("Extraction failed, cleaning up temporary files")
			if removeErr := os.Remove(tempZipPath); removeErr != nil {
				logger.Warnf("Failed to remove temp ZIP after extraction error: %v", removeErr)
			}
			if removeErr := os.RemoveAll(tempExtractDir); removeErr != nil {
				logger.Warnf("Failed to remove temp extract dir after error: %v", removeErr)
			}
			return fmt.Errorf("failed to extract/verify ZIP: %v", err)
		}

		// Remove any existing output directory
		if _, err := os.Stat(finalPath); err == nil {
			logger.Debugf("Removing existing directory: %s", finalPath)
			if err := os.RemoveAll(finalPath); err != nil {
				return fmt.Errorf("failed to remove existing directory: %v", err)
			}
		}

		// Atomic rename from temp extraction to final location
		if err := os.Rename(tempExtractDir, finalPath); err != nil {
			// Clean up on rename failure
			logger.Errorf("Rename failed, cleaning up temporary files")
			if removeErr := os.RemoveAll(tempExtractDir); removeErr != nil {
				logger.Warnf("Failed to remove temp extract dir after rename error: %v", removeErr)
			}
			if removeErr := os.Remove(tempZipPath); removeErr != nil {
				logger.Warnf("Failed to remove temp ZIP after rename error: %v", removeErr)
			}
			return fmt.Errorf("failed to move extracted files: %v", err)
		}

		// Clean up the temporary ZIP file
		if err := os.Remove(tempZipPath); err != nil {
			logger.Warnf("Failed to remove temporary ZIP file %s: %v", tempZipPath, err)
		}

		logger.Debugf("Successfully extracted %s to %s", info.SeriesUID, finalPath)
		return nil
	}
}
