package downloader

import (
	"context"
	"crypto/tls"
	"net"
	"net/http"
	"net/url"
	"time"
)

func newClient(proxy string, maxConnsPerHost int) *http.Client {
	Logger.Debugf("initializing http request client with max %d connections per host", maxConnsPerHost)
	if proxy != "" {
		Logger.Debugf("using proxy %s", proxy)
	}

	// Configure transport for parallel downloads (server-friendly settings)
	transport := &http.Transport{
		MaxIdleConns:          maxConnsPerHost * 2, // Server-friendly: reduced multiplier
		MaxIdleConnsPerHost:   maxConnsPerHost,
		MaxConnsPerHost:       maxConnsPerHost,
		IdleConnTimeout:       30 * time.Second, // Server-friendly: reduced from 90s
		TLSHandshakeTimeout:   20 * time.Second, // Server-friendly: increased timeout
		DisableKeepAlives:     false,            // Enable HTTP/1.1 keep-alive
		DisableCompression:    true,             // Disable compression to avoid issues
		ForceAttemptHTTP2:     false,            // NBIA server doesn't support HTTP/2
		ResponseHeaderTimeout: 30 * time.Second, // Timeout for server response headers
		ExpectContinueTimeout: 1 * time.Second,  // Timeout for HTTP/1.1 100-continue
		TLSClientConfig:       &tls.Config{InsecureSkipVerify: true},
		// Custom dialer with connection timeout
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			dialer := &net.Dialer{
				Timeout:   30 * time.Second, // Connection timeout
				KeepAlive: 30 * time.Second, // TCP keep-alive
			}
			return dialer.DialContext(ctx, network, addr)
		},
	}

	// Add proxy if configured
	if proxy != "" {
		p, err := url.Parse(proxy)
		if err != nil {
			Logger.Fatal("failed to parse proxy string: %v", err)
		}
		transport.Proxy = http.ProxyURL(p)
	}

	client := &http.Client{
		Transport: transport,
		Timeout:   10 * time.Minute, // Global timeout for requests
	}

	return client
}
