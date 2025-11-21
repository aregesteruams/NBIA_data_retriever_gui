package retriever

import (
	"bufio"
	"context"
	"fmt"
	"os/exec"
	"strings"
	"sync"
)

// RunOptions holds the options forwarded from the GUI/CLI
type RunOptions struct {
	MaxConnections     int
	MaxRetries         int
	Processes          int
	SkipExisting       bool
	DownloadInParallel bool // currently informational — CLI doesn't accept this flag
	// Metadata-related
	MetadataWorkers int
	RefreshMetadata bool
	MetaUrl         string
	// Download-related (used by in-process download)
	ImageUrl     string
	NoDecompress bool
	NoMD5        bool
	RetryDelay   int // seconds
	RequestDelay int // milliseconds
	Force        bool
}

// RunFromManifest runs the existing CLI binary with the provided manifest and options.
// It streams textual output lines to logFn as they arrive. The ctx is honored via
// exec.CommandContext so callers can cancel the run.
func RunFromManifest(ctx context.Context, manifestPath, outputDir string, opts RunOptions, logFn func(string)) (string, error) {
	cliPath := "../nbia-data-retriever-cli"
	args := []string{"-i", manifestPath, "--output", outputDir,
		"--max-connections", fmt.Sprintf("%d", opts.MaxConnections),
		"--max-retries", fmt.Sprintf("%d", opts.MaxRetries),
		"--processes", fmt.Sprintf("%d", opts.Processes),
	}
	if opts.SkipExisting {
		args = append(args, "--skip-existing")
	}

	// Note: opts.DownloadInParallel is informational only for now — the CLI does not accept that flag.

	cmd := exec.CommandContext(ctx, cliPath, args...)

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return "", fmt.Errorf("failed to capture stdout: %w", err)
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return "", fmt.Errorf("failed to capture stderr: %w", err)
	}

	if err := cmd.Start(); err != nil {
		return "", fmt.Errorf("failed to start command: %w", err)
	}

	var mu sync.Mutex
	var lines []string
	emit := func(line string) {
		if logFn != nil {
			logFn(line)
		}
		mu.Lock()
		lines = append(lines, line)
		mu.Unlock()
	}

	// Stream stdout
	go func() {
		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			emit(scanner.Text())
		}
	}()

	// Stream stderr
	go func() {
		scanner := bufio.NewScanner(stderr)
		for scanner.Scan() {
			emit(scanner.Text())
		}
	}()

	if err := cmd.Wait(); err != nil {
		mu.Lock()
		combined := strings.Join(lines, "\n")
		mu.Unlock()
		return combined, fmt.Errorf("%w: %s", err, combined)
	}

	mu.Lock()
	combined := strings.Join(lines, "\n")
	mu.Unlock()
	return combined, nil
}
