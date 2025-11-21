package app

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// doRequestFn is the signature for an HTTP request helper (preserves v2->v1 fallback in CLI)
type doRequestFn func(*http.Client, *http.Request) (*http.Response, error)

// DownloadWithRetry downloads a series (zip or direct) with retry logic and optional extraction.
// It operates on retriever.FileInfo and uses the provided httpClient, getAccessToken callback and
// doRequest wrapper. Options are read from opts.
func DownloadWithRetry(ctx context.Context, info *FileInfo, output string, httpClient *http.Client, getAccessToken func() (string, error), doRequest doRequestFn, opts RunOptions, logFn func(string)) error {
	// Server-friendly delay before attempts
	if opts.RequestDelay > 0 {
		time.Sleep(time.Duration(opts.RequestDelay) * time.Millisecond)
	}

	var lastErr error
	delay := time.Duration(opts.RetryDelay) * time.Second
	if delay == 0 {
		delay = 10 * time.Second
	}

	for attempt := 0; attempt <= opts.MaxRetries; attempt++ {
		if attempt > 0 {
			if logFn != nil {
				logFn(fmt.Sprintf("Retrying download %s (attempt %d/%d) after %s delay", info.SeriesUID, attempt, opts.MaxRetries, delay))
			}
			select {
			case <-ctx.Done():
				return fmt.Errorf("cancelled")
			case <-time.After(delay):
			}
			delay *= 2
		}

		err := doDownload(ctx, info, output, httpClient, getAccessToken, doRequest, opts, logFn)
		if err == nil {
			return nil
		}
		lastErr = err
		// Decide if retryable
		if !isRetryableError(err) {
			if logFn != nil {
				logFn(fmt.Sprintf("Non-retryable error for %s: %v", info.SeriesUID, err))
			}
			return err
		}
		if logFn != nil {
			logFn(fmt.Sprintf("Download %s failed (attempt %d/%d): %v", info.SeriesUID, attempt+1, opts.MaxRetries+1, err))
		}
	}

	return fmt.Errorf("download failed after %d attempts: %v", opts.MaxRetries+1, lastErr)
}

func isRetryableError(err error) bool {
	if err == nil {
		return false
	}
	errStr := err.Error()
	return strings.Contains(errStr, "timeout") ||
		strings.Contains(errStr, "connection refused") ||
		strings.Contains(errStr, "connection reset") ||
		strings.Contains(errStr, "EOF") ||
		strings.Contains(errStr, "incomplete download") ||
		strings.Contains(errStr, "closed") ||
		strings.Contains(errStr, "broken pipe") ||
		strings.Contains(errStr, "429") ||
		strings.Contains(errStr, "500") ||
		strings.Contains(errStr, "502") ||
		strings.Contains(errStr, "503") ||
		strings.Contains(errStr, "504")
}

// doDownload dispatches between direct URL downloads and TCIA API downloads
func doDownload(ctx context.Context, info *FileInfo, output string, httpClient *http.Client, getAccessToken func() (string, error), doRequest doRequestFn, opts RunOptions, logFn func(string)) error {
	if info.DownloadURL != "" {
		return downloadDirect(ctx, info, output, httpClient, doRequest, logFn)
	}
	return downloadFromTCIA(ctx, info, output, httpClient, getAccessToken, doRequest, opts, logFn)
}

// downloadDirect downloads a file directly from info.DownloadURL
func downloadDirect(ctx context.Context, info *FileInfo, output string, client *http.Client, doRequest doRequestFn, logFn func(string)) error {
	if logFn != nil {
		logFn(fmt.Sprintf("Downloading direct from URL: %s", info.DownloadURL))
	}

	finalPath := filepath.Join(output, info.SeriesUID)
	tempPath := finalPath + ".tmp"

	// Remove any incomplete temp
	if _, err := os.Stat(tempPath); err == nil {
		_ = os.Remove(tempPath)
	}

	req, err := http.NewRequestWithContext(ctx, "GET", info.DownloadURL, nil)
	if err != nil {
		return err
	}

	resp, err := doRequest(client, req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("HTTP error %d: %s", resp.StatusCode, resp.Status)
	}

	f, err := os.OpenFile(tempPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	defer func() { f.Close(); if err != nil { os.Remove(tempPath) } }()

	buf := bufio.NewReaderSize(resp.Body, 64*1024)
	written, err := io.Copy(f, buf)
	if err != nil {
		return fmt.Errorf("failed to write data after %d bytes: %v", written, err)
	}

	if err := f.Close(); err != nil {
		return err
	}

	if err := os.Rename(tempPath, finalPath); err != nil {
		return err
	}

	if logFn != nil {
		logFn(fmt.Sprintf("Successfully saved %s as %s", info.SeriesUID, finalPath))
	}
	return nil
}

// makeImageURL builds the image download URL (preserves optional ? handling)
func makeImageURL(base string, seriesID string) string {
	if base == "" {
		base = "https://services.cancerimagingarchive.net/nbia-api/services/v2/getImage"
	}
	if strings.Contains(base, "?") {
		return base + "&SeriesInstanceUID=" + seriesID
	}
	return base + "?SeriesInstanceUID=" + seriesID
}

// downloadFromTCIA downloads a ZIP via the API and optionally extracts it
func downloadFromTCIA(ctx context.Context, info *FileInfo, output string, client *http.Client, getAccessToken func() (string, error), doRequest doRequestFn, opts RunOptions, logFn func(string)) error {
	url_ := makeImageURL(opts.ImageUrl, info.SeriesUID)

	// Build request
	req, err := http.NewRequestWithContext(ctx, "GET", url_, nil)
	if err != nil {
		return err
	}

	// Attach token
	if getAccessToken != nil {
		if token, err := getAccessToken(); err == nil && token != "" {
			req.Header.Add("Authorization", "Bearer "+token)
		}
	}

	// Set timeout based on predicted file size
	var timeout time.Duration
	if info.FileSize != "" {
		if fs, err := strconv.ParseInt(info.FileSize, 10, 64); err == nil {
			timeout = 5*time.Minute + time.Duration(fs/(100*1024*1024))*time.Minute
			if timeout > 60*time.Minute {
				timeout = 60 * time.Minute
			}
		}
	}
	if timeout == 0 {
		timeout = 30 * time.Minute
	}

	ctxReq, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	req = req.WithContext(ctxReq)

	resp, err := doRequest(client, req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("HTTP error %d: %s", resp.StatusCode, resp.Status)
	}

	// Determine final paths
	var finalPath string
	var tempZipPath string
	if opts.NoDecompress {
		finalPath = filepath.Join(output, info.SeriesUID)
		tempZipPath = finalPath + ".tmp"
	} else {
		finalPath = filepath.Join(output, info.SeriesUID)
		tempZipPath = finalPath + ".zip.tmp"
	}

	// Clean previous temps
	if _, err := os.Stat(tempZipPath); err == nil {
		_ = os.Remove(tempZipPath)
	}
	if !opts.NoDecompress {
		tempExtract := finalPath + ".uncompressed.tmp"
		_ = os.RemoveAll(tempExtract)
	}

	f, err := os.OpenFile(tempZipPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	defer func() { f.Close(); if err != nil { os.Remove(tempZipPath) } }()

	buf := bufio.NewReaderSize(resp.Body, 64*1024)
	written, err := io.Copy(f, buf)
	if err != nil {
		return fmt.Errorf("failed to write data after %d bytes: %v", written, err)
	}

	if err := f.Close(); err != nil {
		return err
	}

	if opts.NoDecompress {
		if err := os.Rename(tempZipPath, finalPath); err != nil {
			return err
		}
		if logFn != nil {
			logFn(fmt.Sprintf("Successfully saved %s as %s", info.SeriesUID, finalPath))
		}
		return nil
	}

	// Parse MD5s if enabled
	var md5map map[string]string
	if !opts.NoMD5 {
		m, err := ParseMD5HashesCSV(tempZipPath)
		if err != nil {
			if logFn != nil {
				logFn(fmt.Sprintf("Failed to parse MD5 hashes: %v", err))
			}
			md5map = nil
		} else {
			md5map = m
		}
	}

	// Extract into temp dir
	tempExtractDir := finalPath + ".uncompressed.tmp"
	if err := ExtractAndVerifyZip(tempZipPath, tempExtractDir, 0, md5map); err != nil {
		// cleanup
		_ = os.Remove(tempZipPath)
		_ = os.RemoveAll(tempExtractDir)
		return fmt.Errorf("failed to extract/verify ZIP: %v", err)
	}

	// Replace existing
	_ = os.RemoveAll(finalPath)
	if err := os.Rename(tempExtractDir, finalPath); err != nil {
		_ = os.RemoveAll(tempExtractDir)
		_ = os.Remove(tempZipPath)
		return fmt.Errorf("failed to move extracted files: %v", err)
	}
	_ = os.Remove(tempZipPath)

	if logFn != nil {
		logFn(fmt.Sprintf("Successfully extracted %s to %s", info.SeriesUID, finalPath))
	}
	return nil
}
