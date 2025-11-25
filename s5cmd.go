package main

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

// loadS5cmdSeriesMapFromCSVs scans all '*-metadata.csv' files in the metadata
// directory to build a map of previously downloaded s5cmd series.
func loadS5cmdSeriesMapFromCSVs(outputDir string) (map[string]string, error) {
	seriesMap := make(map[string]string)
	metaDir := filepath.Join(outputDir, "metadata")

	files, err := os.ReadDir(metaDir)
	if err != nil {
		if os.IsNotExist(err) {
			return seriesMap, nil // No metadata dir yet, so no map.
		}
		return nil, fmt.Errorf("could not read metadata directory: %w", err)
	}

	for _, file := range files {
		if file.IsDir() || !strings.HasSuffix(file.Name(), "-metadata.csv") {
			continue
		}

		filePath := filepath.Join(metaDir, file.Name())
		f, err := os.Open(filePath)
		if err != nil {
			logger.Warnf("Could not open metadata CSV %s: %v", filePath, err)
			continue
		}
		defer f.Close()

		reader := csv.NewReader(f)
		header, err := reader.Read()
		if err != nil {
			logger.Warnf("Could not read header from CSV %s: %v", filePath, err)
			continue
		}

		uriIndex, uidIndex := -1, -1
		for i, colName := range header {
			if colName == "OriginalS5cmdURI" {
				uriIndex = i
			} else if colName == "SeriesInstanceUID" {
				uidIndex = i
			}
		}

		if uriIndex == -1 || uidIndex == -1 {
			logger.Warnf("Could not find required columns in %s", filePath)
			continue
		}

		for {
			record, err := reader.Read()
			if err == io.EOF {
				break
			}
			if err != nil {
				logger.Warnf("Error reading record from %s: %v", filePath, err)
				continue
			}
			if len(record) > uriIndex && len(record) > uidIndex {
				seriesMap[record[uriIndex]] = record[uidIndex]
			}
		}
	}

	return seriesMap, nil
}

func decodeS5cmd(filePath string, outputDir string, processedSeries map[string]string) ([]*FileInfo, int) {
	file, err := os.Open(filePath)
	if err != nil {
		logger.Fatalf("could not open s5cmd manifest: %v", err)
	}
	defer file.Close()

	var jobsToProcess []*FileInfo
	var newJobs int
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.Fields(line)
		var originalURI string
		if len(parts) >= 2 && parts[0] == "cp" {
			originalURI = parts[1]
		} else if len(parts) == 1 && strings.HasPrefix(parts[0], "s3://") {
			originalURI = parts[0]
		} else {
			continue // Skip comments and invalid lines
		}

		if seriesUID, ok := processedSeries[originalURI]; ok {
			// This is a sync job for an existing series
			logger.Infof("Queueing sync job for existing series: %s", originalURI)
			finalDirPath := filepath.Join(outputDir, seriesUID)
			jobsToProcess = append(jobsToProcess, &FileInfo{
				DownloadURL:      originalURI,
				SeriesUID:        seriesUID, // We already know the final UID
				OriginalS5cmdURI: originalURI,
				S5cmdManifestPath: finalDirPath, // The final directory is the target for sync
				IsSyncJob:        true,
			})
		} else {
			// This is a new copy job
			newJobs++
			logger.Infof("Queueing new copy job for series: %s", originalURI)
			cleanURI := strings.TrimSuffix(originalURI, "/*")
			seriesGUID := filepath.Base(cleanURI)
			tempDirName := "s5cmd-tmp-" + seriesGUID
			tempDirPath := filepath.Join(outputDir, tempDirName)

			if err := os.MkdirAll(tempDirPath, 0755); err != nil {
				logger.Warnf("Could not create temp directory for %s: %v", originalURI, err)
				continue
			}

			jobsToProcess = append(jobsToProcess, &FileInfo{
				DownloadURL:      originalURI,
				SeriesUID:        filepath.Base(originalURI), // Temporary ID for progress
				OriginalS5cmdURI: originalURI,
				S5cmdManifestPath: tempDirPath, // The temporary directory is the target for copy
				IsSyncJob:        false,
			})
		}
	}

	if err := scanner.Err(); err != nil {
		logger.Fatalf("error reading s5cmd manifest: %v", err)
	}

	logger.Infof("Found %d s5cmd jobs to process (%d new, %d existing)", len(jobsToProcess), newJobs, len(jobsToProcess)-newJobs)
	return jobsToProcess, newJobs
}
