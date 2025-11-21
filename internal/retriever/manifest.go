package retriever

import (
	"bufio"
	"os"
	"strings"
)

// ParseManifest reads a TCIA manifest file (a .tcia file) and returns the list
// of SeriesInstanceUIDs found in the file. Lines containing '=' are ignored
// (they are treated as key=value comments in some manifests).
func ParseManifest(path string) ([]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	ids := make([]string, 0)
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		if strings.ContainsAny(line, "=") {
			// Skip comment/metadata lines
			continue
		}
		ids = append(ids, line)
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return ids, nil
}
