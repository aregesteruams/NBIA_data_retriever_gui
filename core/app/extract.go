package app

import (
    "archive/zip"
    "encoding/csv"
    "encoding/hex"
    "crypto/md5"
    "fmt"
    "hash"
    "io"
    "os"
    "path/filepath"
    "strings"
)

// ExtractAndVerifyZip extracts a zip archive at zipPath into destDir, verifies
// uncompressed size against expectedSize when provided, and optionally validates
// MD5 hashes using md5Map (map[fileName]md5Hash).
func ExtractAndVerifyZip(zipPath string, destDir string, expectedSize int64, md5Map map[string]string) error {
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

    md5Mode := len(md5Map) > 0

    for _, file := range reader.File {
        if file.Name == "md5hashes.csv" {
            continue
        }

        path := filepath.Join(destDir, file.Name)

        // Ensure path is inside destDir
        if !strings.HasPrefix(path, filepath.Clean(destDir)+string(os.PathSeparator)) {
            return fmt.Errorf("invalid file path in zip: %s", file.Name)
        }

        if file.FileInfo().IsDir() {
            if err := os.MkdirAll(path, file.Mode()); err != nil {
                return fmt.Errorf("failed to create directory: %v", err)
            }
            continue
        }

        if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
            return fmt.Errorf("failed to create file directory: %v", err)
        }

        fileReader, err := file.Open()
        if err != nil {
            return fmt.Errorf("failed to open file in zip: %v", err)
        }

        targetFile, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, file.Mode())
        if err != nil {
            fileReader.Close()
            return fmt.Errorf("failed to create file: %v", err)
        }

        isImagingFile := false
        expectedMD5 := ""
        if md5Hash, ok := md5Map[file.Name]; ok {
            isImagingFile = true
            expectedMD5 = md5Hash
        }

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

        if hasher != nil && expectedMD5 != "" {
            actualMD5 := hex.EncodeToString(hasher.Sum(nil))
            if actualMD5 != expectedMD5 {
                md5Errors = append(md5Errors, fmt.Sprintf("%s: expected %s, got %s", file.Name, expectedMD5, actualMD5))
            }
        }

        if md5Mode {
            if isImagingFile {
                totalSize += written
            }
        } else {
            totalSize += written
        }
    }

    if len(md5Errors) > 0 {
        return fmt.Errorf("MD5 validation failed for %d files:\n%s", len(md5Errors), strings.Join(md5Errors, "\n"))
    }

    if expectedSize > 0 && totalSize != expectedSize {
        if md5Mode {
            return fmt.Errorf("size mismatch: expected %d bytes, extracted %d bytes", expectedSize, totalSize)
        }
    }

    return nil
}

// ParseMD5HashesCSV reads md5hashes.csv inside a zip archive and returns a map filename->md5
func ParseMD5HashesCSV(zipPath string) (map[string]string, error) {
    reader, err := zip.OpenReader(zipPath)
    if err != nil {
        return nil, fmt.Errorf("failed to open zip: %v", err)
    }
    defer reader.Close()

    for _, file := range reader.File {
        if file.Name == "md5hashes.csv" {
            rc, err := file.Open()
            if err != nil {
                return nil, fmt.Errorf("failed to open md5hashes.csv: %v", err)
            }
            defer rc.Close()

            csvReader := csv.NewReader(rc)
            records, err := csvReader.ReadAll()
            if err != nil {
                return nil, fmt.Errorf("failed to parse CSV: %v", err)
            }

            md5Map := make(map[string]string)
            for i, record := range records {
                if i == 0 || len(record) < 2 {
                    continue
                }
                md5Map[record[0]] = record[1]
            }
            return md5Map, nil
        }
    }
    return nil, fmt.Errorf("md5hashes.csv not found in ZIP")
}

// GetDirectorySize returns the total size of regular files in a directory
func GetDirectorySize(dirPath string) (int64, error) {
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
