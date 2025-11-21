package app 

import (
	"net/http"
	"strings"
)

// doRequest performs an HTTP request with automatic v2 -> v1 fallback
// This provides a graceful degradation when v2 endpoints are unavailable
func doRequest(client *http.Client, req *http.Request) (*http.Response, error) {
	// Save original URL for potential fallback
	originalURL := req.URL.String()

	// Try the request as-is
	resp, err := client.Do(req)

	// If successful or not a v2 endpoint, return as-is
	if err != nil || !strings.Contains(originalURL, "/v2/") {
		return resp, err
	}

	// Check if we should fallback to v1
	// Fallback on: 404 (endpoint not found), 500-504 (server errors), 502 (bad gateway)
	if resp.StatusCode == 404 || (resp.StatusCode >= 500 && resp.StatusCode <= 504) {
		Logger.Warnf("v2 endpoint returned %d, falling back to v1: %s", resp.StatusCode, originalURL)
		resp.Body.Close()

		// Create v1 URL
		v1URL := strings.Replace(originalURL, "/v2/", "/v1/", 1)

		// Create new request with v1 URL
		v1Req, err := http.NewRequest(req.Method, v1URL, req.Body)
		if err != nil {
			return nil, err
		}

		// Copy headers
		v1Req.Header = req.Header.Clone()

		// Copy context if present
		if req.Context() != nil {
			v1Req = v1Req.WithContext(req.Context())
		}

		// Try v1 endpoint
		Logger.Infof("Attempting v1 endpoint: %s", v1URL)
		return client.Do(v1Req)
	}

	// Return original response for other status codes
	return resp, nil
}
