package main

import (
	"archive/tar"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/rs/zerolog/log"
)

/*
UnTar takes a destination path and a reader; a tar reader loops over the tarfile
creating the file structure at 'dst' along the way, and writing any files
*/
func UnTar(dst string, r io.Reader) error {

	tr := tar.NewReader(r)

	for {
		header, err := tr.Next()

		switch {

		// if no more files are found return
		case err == io.EOF:
			return nil

		// return any other error
		case err != nil:
			return err

		// if the header is nil, just skip it (not sure how this happens)
		case header == nil:
			continue
		}

		// the target location where the dir/file should be created
		target := filepath.Join(dst, header.Name)

		// the following switch could also be done using fi.Mode(), not sure if there
		// a benefit of using one vs. the other.
		// fi := header.FileInfo()

		// check the file type
		switch header.Typeflag {

		// if its a dir and it doesn't exist create it
		case tar.TypeDir:
			if _, err := os.Stat(target); err != nil {
				if err := os.MkdirAll(target, 0755); err != nil {
					return err
				}
			}

		// if it's a file create it
		case tar.TypeReg:
			f, err := os.OpenFile(target, os.O_CREATE|os.O_RDWR, os.FileMode(header.Mode))
			if err != nil {
				return err
			}

			// copy over contents
			if _, err := io.Copy(f, tr); err != nil {
				return err
			}

			// manually close here after each file operation; defering would cause each file close
			// to wait until all operations have completed.
			f.Close()
		}
	}
}

// ToJSON as name says
func ToJSON(files []*FileInfo, output string) {
	rankingsJSON, _ := json.MarshalIndent(files, "", "    ")
	err := os.WriteFile(output, rankingsJSON, 0644)

	if err != nil {
		log.Error().Msgf("%v", err)
	}
}

// writeMetadataToCSV writes/appends a slice of FileInfo structs to a CSV file.
func writeMetadataToCSV(filePath string, fileInfos []*FileInfo) error {
	// Check if file exists to determine if we need to write a header
	stat, err := os.Stat(filePath)
	writeHeader := os.IsNotExist(err)

	// Open with read/write/create permissions.
	file, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return fmt.Errorf("could not open/create CSV file: %w", err)
	}
	defer file.Close()

	// If the file is not new and not empty, check for a trailing newline.
	if !writeHeader && stat.Size() > 0 {
		buf := make([]byte, 1)
		// Read the last byte.
		if _, err := file.ReadAt(buf, stat.Size()-1); err == nil {
			// If it's not a newline, we need to add one.
			if buf[0] != '\n' {
				// Seek to the end and write a newline.
				if _, err := file.Seek(0, io.SeekEnd); err != nil {
					return fmt.Errorf("could not seek to end of file to add newline: %w", err)
				}
				if _, err := file.WriteString("\n"); err != nil {
					return fmt.Errorf("failed to write missing newline: %w", err)
				}
			}
		}
	}

	// Ensure we are at the end of the file before letting the CSV writer take over.
	if _, err := file.Seek(0, io.SeekEnd); err != nil {
		return fmt.Errorf("could not seek to end of file for writing: %w", err)
	}

	writer := csv.NewWriter(file)
	defer writer.Flush()

	header := []string{
		"SeriesInstanceUID", "SubjectID", "Collection", "Modality",
		"StudyInstanceUID", "SeriesDescription", "SeriesNumber",
		"Manufacturer", "NumberOfImages", "FileSize", "MD5Hash",
		"OriginalS5cmdURI",
	}

	if writeHeader {
		if err := writer.Write(header); err != nil {
			return fmt.Errorf("failed to write CSV header: %w", err)
		}
	}

	// Write rows
	for _, info := range fileInfos {
		record := []string{
			info.SeriesUID,
			info.SubjectID,
			info.Collection,
			info.Modality,
			info.StudyUID,
			info.SeriesDescription,
			info.SeriesNumber,
			info.Manufacturer,
			info.NumberOfImages,
			info.FileSize,
			info.MD5Hash,
			info.OriginalS5cmdURI,
		}
		if err := writer.Write(record); err != nil {
			return fmt.Errorf("failed to write CSV record for series %s: %w", info.SeriesUID, err)
		}
	}

	return nil
}

// copyFile copies a file from src to dst.
func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()

	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer out.Close()

	_, err = io.Copy(out, in)
	if err != nil {
		return err
	}
	return out.Close()
}
