package app 

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

)

var (
	// Directory creation mutex
	dirMutex sync.Mutex
	// Metadata cache mutex
	metaMutex sync.Mutex
	// Forward logs from the retriever to the frontend runtime events
)


// CreateMetadataDir creates the metadata directory if it doesn't exist
func CreateMetadataDir(output string) error {
	metaDir := filepath.Join(output, "metadata")
	if _, err := os.Stat(metaDir); os.IsNotExist(err) {
		return os.MkdirAll(metaDir, 0755)
	}
	return nil
}


// DecodeTCIA is used to decode the tcia file with parallel metadata fetching
func DecodeTCIA(path string, httpClient *http.Client, authToken *Token, options *Options) []*FileInfo {
	Logger.Debugf("decoding tcia file: %s", path)

	f, err := os.Open(path)
	if err != nil {
		Logger.Fatal(err)
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
		Logger.Errorf("error reading tcia file: %v", err)
	}

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
						Logger.Debugf("[Meta Worker %d] Loaded metadata from cache for: %s", workerID, seriesID)
						mu.Lock()
						results = append(results, cachedInfo)
						mu.Unlock()
						metaStats.updateProgress(nil, "cached", seriesID)
						continue
					}
					// Cache miss or error, fetch from API
					Logger.Debugf("[Meta Worker %d] Cache miss, fetching metadata for: %s", workerID, seriesID)
				} else {
					Logger.Debugf("[Meta Worker %d] Force refresh, fetching metadata for: %s", workerID, seriesID)
				}

				url_, err := makeURL(MetaUrl, map[string]interface{}{"SeriesInstanceUID": seriesID})
				if err != nil {
					Logger.Errorf("[Meta Worker %d] Failed to make URL: %v", workerID, err)
					metaStats.updateProgress(nil, "failed", seriesID)
					continue
				}

				req, err := http.NewRequest("GET", url_, nil)
				if err != nil {
					Logger.Errorf("[Meta Worker %d] Failed to create request: %v", workerID, err)
					metaStats.updateProgress(nil, "failed", seriesID)
					continue
				}

				// Get current access token
				accessToken, err := authToken.GetAccessToken()
				if err != nil {
					Logger.Errorf("[Meta Worker %d] Failed to get access token: %v", workerID, err)
					metaStats.updateProgress(nil, "failed", seriesID)
					continue
				}
				req.Header.Add("Authorization", fmt.Sprintf("Bearer %s", accessToken))

				// Set timeout for metadata request
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				req = req.WithContext(ctx)

				resp, err := doRequest(httpClient, req)
				cancel() // Cancel context after request
				if err != nil {
					Logger.Errorf("[Meta Worker %d] Failed to do request: %v", workerID, err)
					metaStats.updateProgress(nil, "failed", seriesID)
					continue
				}

				content, err := io.ReadAll(resp.Body)
				_ = resp.Body.Close()
				if err != nil {
					Logger.Errorf("[Meta Worker %d] Failed to read response data: %v", workerID, err)
					metaStats.updateProgress(nil, "failed", seriesID)
					continue
				}

				files := make([]*FileInfo, 0)
				err = json.Unmarshal(content, &files)
				if err != nil {
					Logger.Errorf("[Meta Worker %d] Failed to parse response data: %v", workerID, err)
					Logger.Debugf("%s", content)
					metaStats.updateProgress(nil, "failed", seriesID)
					continue
				}

				// Save to cache - usually one file per series
				for _, file := range files {
					if file.SeriesUID != "" {
						if err := saveMetadataToCache(file, getMetadataCachePath(options.Output, file.SeriesUID)); err != nil {
							Logger.Warnf("[Meta Worker %d] Failed to cache metadata for %s: %v", workerID, file.SeriesUID, err)
						}
					}
				}

				// Thread-safe append to results
				mu.Lock()
				results = append(results, files...)
				mu.Unlock()

				// Mark as successfully fetched
				metaStats.updateProgress(nil, "fetched", seriesID)
			}
		}(i + 1)
	}

	// Wait for all workers to finish
	wg.Wait()

	fmt.Printf("Successfully fetched metadata for %d files\n", len(results))
	return results
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
			Logger.Fatal(err)
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
		Logger.Debugf("Force flag set, will re-download %s", info.SeriesUID)
		return true
	}

	var targetPath string
	if info.DownloadURL != "" {
		targetPath = filepath.Join(output, info.SeriesUID)
		_, err := os.Stat(targetPath)
		if os.IsNotExist(err) {
			Logger.Debugf("Target %s does not exist, need to download", targetPath)
			return true
		}
		// If it exists, we assume it's downloaded. We don't have size/checksum info for these.
		Logger.Debugf("Direct download file %s exists, skipping", targetPath)
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
			Logger.Debugf("Target %s does not exist, need to download", targetPath)
			return true
		}
		Logger.Warnf("Error checking target %s: %v", targetPath, err)
		return true
	}

	if noDecompress {
		// For ZIP files, check if it's a regular file
		if stat.IsDir() {
			Logger.Debugf("%s exists but is a directory, need to re-download", targetPath)
			return true
		}
		// For ZIP files, we can't easily verify the size as it's compressed
		// Just check existence for now
		Logger.Debugf("ZIP file %s exists, skipping", targetPath)
		return false
	} else {
		// For extracted files, check if it's a directory
		if !stat.IsDir() {
			Logger.Debugf("%s exists but is not a directory, need to re-download", targetPath)
			return true
		}

		// Check total size of extracted files
		if info.FileSize != "" {
			expectedSize, err := strconv.ParseInt(info.FileSize, 10, 64)
			if err == nil {
				actualSize, err := GetDirectorySize(targetPath)
				if err != nil {
					Logger.Warnf("Error calculating directory size for %s: %v", targetPath, err)
					return true
				}
				if actualSize != expectedSize {
					Logger.Debugf("Directory %s size mismatch: expected %d, got %d", targetPath, expectedSize, actualSize)
					return true
				}
			}
		}

		Logger.Debugf("Directory %s exists with correct size, skipping", targetPath)
		return false
	}
}


func (info *FileInfo) GetMeta(output string) error {
	Logger.Debugf("getting meta information and save to %s", output)
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
func (info *FileInfo) Download(ctx context.Context, output string, httpClient *http.Client, authToken *Token, options *Options) error {
	// Add rate limiting delay between requests
	if options.RequestDelay > 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(options.RequestDelay):
		}
	}
	return info.DownloadWithRetry(ctx, output, httpClient, authToken, options)
}

// DownloadWithRetry downloads file with retry logic and exponential backoff
func (info *FileInfo) DownloadWithRetry(ctx context.Context, output string, httpClient *http.Client, authToken *Token, options *Options) error {
	// Convert to retriever.FileInfo and delegate to retriever.DownloadWithRetry
	rf := &FileInfo{
		NumberOfImages:     info.NumberOfImages,
		SOPClassUID:        info.SOPClassUID,
		Manufacturer:       info.Manufacturer,
		DataDescriptionURI: info.DataDescriptionURI,
		LicenseURL:         info.LicenseURL,
		AnnotationSize:     info.AnnotationSize,
		Collection:         info.Collection,
		StudyDescription:   info.StudyDescription,
		SeriesUID:          info.SeriesUID,
		StudyUID:           info.StudyUID,
		LicenseName:        info.LicenseName,
		StudyDate:          info.StudyDate,
		SeriesDescription:  info.SeriesDescription,
		Modality:           info.Modality,
		RdPartyAnalysis:    info.RdPartyAnalysis,
		FileSize:           info.FileSize,
		SubjectID:          info.SubjectID,
		SeriesNumber:       info.SeriesNumber,
		MD5Hash:            info.MD5Hash,
		DownloadURL:        info.DownloadURL,
	}

	// Build retriever options mapping
	rOpts := RunOptions{
		MaxRetries:   options.MaxRetries,
		RetryDelay:   int(options.RetryDelay / time.Second),
		RequestDelay: int(options.RequestDelay / time.Millisecond),
		NoDecompress: options.NoDecompress,
		NoMD5:        options.NoMD5,
		ImageUrl:     options.ImageUrl,
		Force:        options.Force,
	}

	// Helper to get token
	getToken := func() (string, error) {
		if authToken == nil {
			return "", fmt.Errorf("no token available")
		}
		return authToken.GetAccessToken()
	}

	// Delegate to retriever implementation
	return DownloadWithRetry(ctx, rf, output, httpClient, getToken, doRequest, rOpts, func(line string) {
		// Forward retriever logs to CLI logger
		Logger.Infof("[retriever] %s", line)
	})
}


// doDownload is a dispatcher for different download types
func (info *FileInfo) doDownload(output string, httpClient *http.Client, authToken *Token, options *Options) error {
	if info.DownloadURL != "" {
		return info.downloadDirect(output, httpClient, options)
	}
	return info.downloadFromTCIA(output, httpClient, authToken, options)
}

// downloadDirect downloads a file from a direct URL without decompression
func (info *FileInfo) downloadDirect(output string, httpClient *http.Client, options *Options) error {
	Logger.Debugf("Downloading direct from URL: %s", info.DownloadURL)

	finalPath := filepath.Join(output, info.SeriesUID)
	tempPath := finalPath + ".tmp"

	// Clean up any previous temporary files
	if _, err := os.Stat(tempPath); err == nil {
		Logger.Debugf("Removing incomplete download: %s", tempPath)
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

	Logger.Debugf("Downloaded %d bytes for %s", written, info.SeriesUID)

	if err := f.Close(); err != nil {
		return fmt.Errorf("failed to close file: %v", err)
	}

	// Atomic rename to final location
	if err := os.Rename(tempPath, finalPath); err != nil {
		return fmt.Errorf("failed to move file: %v", err)
	}

	Logger.Debugf("Successfully saved %s as %s", info.SeriesUID, finalPath)
	return nil
}

// downloadFromTCIA performs the actual download from TCIA, with decompression
func (info *FileInfo) downloadFromTCIA(output string, httpClient *http.Client, authToken *Token, options *Options) error {
	Logger.Debugf("getting image file to %s", output)

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
		Logger.Debugf("Removing incomplete download: %s", tempZipPath)
		os.Remove(tempZipPath)
	}

	// For extraction mode, also clean up temporary extraction directory
	if !options.NoDecompress {
		tempExtractDir := finalPath + ".uncompressed.tmp"
		if _, err := os.Stat(tempExtractDir); err == nil {
			Logger.Debugf("Removing incomplete extraction: %s", tempExtractDir)
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
	Logger.Debugf("Setting download timeout to %v for %s", timeout, info.SeriesUID)
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	req = req.WithContext(ctx)

	resp, err := doRequest(httpClient, req)
	if err != nil {
		return fmt.Errorf("failed to do request: %v", err)
	}
	defer resp.Body.Close()

	// Log response headers for debugging
	Logger.Debugf("Response headers for %s: Status=%s, Content-Length=%d, Transfer-Encoding=%s",
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
		Logger.Debugf("Downloading %s (size: %d bytes)", info.SeriesUID, resp.ContentLength)
	} else {
		Logger.Debugf("Downloading %s (size: unknown)", info.SeriesUID)
	}

	// Buffer the response body for better handling of chunked transfers
	bufferedReader := bufio.NewReaderSize(resp.Body, 64*1024) // 64KB buffer

	// Download without progress bar
	written, err := io.Copy(f, bufferedReader)
	if err != nil {
		// Log detailed error information
		Logger.Errorf("Download error for %s: %v (written=%d bytes)", info.SeriesUID, err, written)
		// Check if it's an EOF error (connection closed)
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			Logger.Errorf("Connection closed prematurely by server for %s", info.SeriesUID)
		}
		return fmt.Errorf("failed to write data after %d bytes: %v", written, err)
	}

	Logger.Debugf("Downloaded %d bytes for %s", written, info.SeriesUID)

	// Note: FileSize in manifest is the uncompressed size, but we download ZIP files
	// So we cannot validate the downloaded size against FileSize
	// Log the download completion instead
	if info.FileSize != "" {
		expectedSize, _ := strconv.ParseInt(info.FileSize, 10, 64)
		compressionRatio := float64(written) / float64(expectedSize) * 100
		Logger.Debugf("Downloaded %s: %d bytes (%.1f%% of uncompressed size %d)",
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
			Logger.Debugf("Removing existing file: %s", finalPath)
			if err := os.Remove(finalPath); err != nil {
				return fmt.Errorf("failed to remove existing file: %v", err)
			}
		}

		// Atomic rename from temp to final location
		if err := os.Rename(tempZipPath, finalPath); err != nil {
			return fmt.Errorf("failed to move ZIP file: %v", err)
		}

		Logger.Debugf("Successfully saved %s as %s", info.SeriesUID, finalPath)
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
			md5Map, err = ParseMD5HashesCSV(tempZipPath)
			if err != nil {
				Logger.Warnf("Failed to parse MD5 hashes: %v", err)
				// Continue without MD5 validation
				md5Map = nil
			}
		}

		Logger.Debugf("Extracting %s to %s", tempZipPath, tempExtractDir)
		if err := ExtractAndVerifyZip(tempZipPath, tempExtractDir, expectedSize, md5Map); err != nil {
			// Clean up temp files on extraction failure
			Logger.Errorf("Extraction failed, cleaning up temporary files")
			if removeErr := os.Remove(tempZipPath); removeErr != nil {
				Logger.Warnf("Failed to remove temp ZIP after extraction error: %v", removeErr)
			}
			if removeErr := os.RemoveAll(tempExtractDir); removeErr != nil {
				Logger.Warnf("Failed to remove temp extract dir after error: %v", removeErr)
			}
			return fmt.Errorf("failed to extract/verify ZIP: %v", err)
		}

		// Remove any existing output directory
		if _, err := os.Stat(finalPath); err == nil {
			Logger.Debugf("Removing existing directory: %s", finalPath)
			if err := os.RemoveAll(finalPath); err != nil {
				return fmt.Errorf("failed to remove existing directory: %v", err)
			}
		}

		// Atomic rename from temp extraction to final location
		if err := os.Rename(tempExtractDir, finalPath); err != nil {
			// Clean up on rename failure
			Logger.Errorf("Rename failed, cleaning up temporary files")
			if removeErr := os.RemoveAll(tempExtractDir); removeErr != nil {
				Logger.Warnf("Failed to remove temp extract dir after rename error: %v", removeErr)
			}
			if removeErr := os.Remove(tempZipPath); removeErr != nil {
				Logger.Warnf("Failed to remove temp ZIP after rename error: %v", removeErr)
			}
			return fmt.Errorf("failed to move extracted files: %v", err)
		}

		// Clean up the temporary ZIP file
		if err := os.Remove(tempZipPath); err != nil {
			Logger.Warnf("Failed to remove temporary ZIP file %s: %v", tempZipPath, err)
		}

		Logger.Debugf("Successfully extracted %s to %s", info.SeriesUID, finalPath)
		return nil
	}
}
