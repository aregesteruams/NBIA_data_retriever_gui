package retriever

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// Run executes the pipeline in-process: fetch metadata, then orchestrate downloads.
// It honors the provided context for cancellation and streams log lines via logFn.
// Run executes the pipeline in-process: fetch metadata, then orchestrate downloads.
// It honors the provided context for cancellation and streams log lines via logFn.
// The getAccessToken callback, if non-nil, is used to obtain OAuth tokens for
// protected endpoints.
func Run(ctx context.Context, manifestPath, outputDir string, opts RunOptions, getAccessToken func() (string, error), logFn func(string)) (string, error) {
	// Create a basic HTTP client. We set MaxConnsPerHost if provided.
	client := &http.Client{}

	// Use provided getAccessToken or a default no-op implementation
	getToken := getAccessToken
	if getToken == nil {
		getToken = func() (string, error) { return "", nil }
	}

	if logFn != nil {
		logFn(fmt.Sprintf("Starting in-process run for manifest %s -> %s", manifestPath, outputDir))
	}

	// Ensure output directories exist
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return "", err
	}

	// 1) Fetch metadata
	metas, err := FetchMetadata(ctx, manifestPath, outputDir, client, getToken, opts, logFn)
	if err != nil {
		return "", fmt.Errorf("metadata fetch failed: %w", err)
	}

	// Convert metadata into Downloadable adapters
	items := make([]Downloadable, 0, len(metas))
	// Build a simple doRequest wrapper that implements a v2->v1 fallback similar
	// to the project's HTTP helper. This is a lightweight fallback used when the
	// retriever package is running in-process.
	doRequestLocal := func(c *http.Client, req *http.Request) (*http.Response, error) {
		originalURL := req.URL.String()
		resp, err := c.Do(req)
		if err != nil || !strings.Contains(originalURL, "/v2/") {
			return resp, err
		}
		if resp != nil && (resp.StatusCode == 404 || (resp.StatusCode >= 500 && resp.StatusCode <= 504)) {
			// try v1 fallback
			resp.Body.Close()
			v1URL := strings.Replace(originalURL, "/v2/", "/v1/", 1)
			v1Req, err := http.NewRequestWithContext(req.Context(), req.Method, v1URL, req.Body)
			if err != nil {
				return nil, err
			}
			v1Req.Header = req.Header.Clone()
			return c.Do(v1Req)
		}
		return resp, nil
	}

	for _, m := range metas {
		// Ensure metadata file is saved via existing helper
		if err := saveMetadataToCache(m, getMetadataCachePath(outputDir, m.SeriesUID)); err != nil {
			if logFn != nil {
				logFn(fmt.Sprintf("warning: failed to save metadata cache for %s: %v", m.SeriesUID, err))
			}
		}
		adapter := &fiAdapter{fi: m, opts: opts, client: client, getToken: getToken, doRequest: doRequestLocal}
		items = append(items, adapter)
	}

	// 2) Orchestrate downloads (metaOnly=false)
	summary, err := OrchestrateDownloads(ctx, items, outputDir, opts, false, logFn)
	if err != nil {
		return "", fmt.Errorf("orchestration failed: %w", err)
	}

	// Build a simple textual summary to return to callers
	summaryStr := fmt.Sprintf("Completed: total=%d downloaded=%d skipped=%d failed=%d elapsed=%s",
		summary.Total, summary.Downloaded, summary.Skipped, summary.Failed, summary.Elapsed.String())

	return summaryStr, nil
}

// fiAdapter adapts retriever.FileInfo to the Downloadable interface used by the orchestrator.
type fiAdapter struct {
	fi        *FileInfo
	opts      RunOptions
	client    *http.Client
	getToken  func() (string, error)
	doRequest func(*http.Client, *http.Request) (*http.Response, error)
}

func (a *fiAdapter) SeriesID() string {
	if a.fi == nil {
		return "<nil>"
	}
	return a.fi.SeriesUID
}

func (a *fiAdapter) GetMeta(output string) error {
	if a.fi == nil {
		return fmt.Errorf("no file info")
	}
	return saveMetadataToCache(a.fi, getMetadataCachePath(output, a.fi.SeriesUID))
}

func (a *fiAdapter) NeedsDownload(output string) bool {
	if a.opts.Force {
		return true
	}
	if a.fi.DownloadURL != "" {
		target := filepath.Join(output, a.fi.SeriesUID)
		if _, err := os.Stat(target); os.IsNotExist(err) {
			return true
		}
		return false
	}
	if a.opts.NoDecompress {
		target := filepath.Join(output, a.fi.SeriesUID) + ".zip"
		if _, err := os.Stat(target); os.IsNotExist(err) {
			return true
		}
		return false
	}
	targetDir := filepath.Join(output, a.fi.SubjectID, a.fi.StudyUID, a.fi.SeriesUID)
	if st, err := os.Stat(targetDir); err != nil || !st.IsDir() {
		return true
	}
	// If FileSize is known, try basic size check
	if a.fi.FileSize != "" {
		if expected, err := strconv.ParseInt(a.fi.FileSize, 10, 64); err == nil {
			if actual, err := GetDirectorySize(targetDir); err == nil {
				if actual != expected {
					return true
				}
			}
		}
	}
	return false
}

func (a *fiAdapter) Download(ctx context.Context, output string) error {
	client := a.client
	if client == nil {
		client = &http.Client{}
	}
	getToken := a.getToken
	if getToken == nil {
		getToken = func() (string, error) { return "", nil }
	}
	doReq := a.doRequest
	if doReq == nil {
		doReq = func(c *http.Client, r *http.Request) (*http.Response, error) { return c.Do(r) }
	}
	return DownloadWithRetry(ctx, a.fi, output, client, getToken, doReq, a.opts, nil)
}
