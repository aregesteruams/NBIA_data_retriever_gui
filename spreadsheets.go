package main

import (
	"encoding/csv"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/tealeg/xlsx"
)

// SpreadSheetDecoder is an interface for decoding spreadsheets
type SpreadSheetDecoder interface {
	Decode(file *os.File) ([][]string, error)
}

// CSVDecoder decodes CSV files
type CSVDecoder struct{}

// TSVDecoder decodes TSV files
type TSVDecoder struct{}

// XLSXDecoder decodes XLSX files
type XLSXDecoder struct{}

// Decode decodes a CSV file and returns the values from the "imageUrl" or "drs_uri" column
func (d *CSVDecoder) Decode(file *os.File) ([][]string, error) {
	return decodesv(file, ',')
}

// Decode decodes a TSV file and returns the values from the "imageUrl" or "drs_uri" column
func (d *TSVDecoder) Decode(file *os.File) ([][]string, error) {
	return decodesv(file, '\t')
}

// decodesv decodes a separated value file and returns the values from the "imageUrl" or "drs_uri" column
func decodesv(file *os.File, separator rune) ([][]string, error) {
	reader := csv.NewReader(file)
	reader.Comma = separator
	records, err := reader.ReadAll()
	if err != nil {
		return nil, err
	}
	return records, nil
}

// Decode decodes an XLSX file and returns the values from the "imageUrl" or "drs_uri" column
func (d *XLSXDecoder) Decode(file *os.File) ([][]string, error) {
	stat, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("could not get file stats: %w", err)
	}
	size := stat.Size()
	xlFile, err := xlsx.OpenReaderAt(file, size)
	if err != nil {
		return nil, err
	}

	var records [][]string
	for _, sheet := range xlFile.Sheets {
		for _, row := range sheet.Rows {
			var record []string
			for _, cell := range row.Cells {
				record = append(record, cell.String())
			}
			records = append(records, record)
		}
	}
	return records, nil
}

// getSpreadsheetDecoder returns a decoder based on the file extension
func getSpreadsheetDecoder(filename string) (SpreadSheetDecoder, error) {
	ext := strings.ToLower(filepath.Ext(filename))
	switch ext {
	case ".csv":
		return &CSVDecoder{}, nil
	case ".tsv":
		return &TSVDecoder{}, nil
	case ".xlsx":
		return &XLSXDecoder{}, nil
	default:
		return nil, fmt.Errorf("unsupported spreadsheet format: %s", ext)
	}
}

// decodeSpreadsheet decodes a spreadsheet file and returns a slice of FileInfo objects
func decodeSpreadsheet(filePath string) ([]*FileInfo, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	decoder, err := getSpreadsheetDecoder(filePath)
	if err != nil {
		return nil, err
	}

	records, err := decoder.Decode(file)
	if err != nil {
		return nil, err
	}

	if len(records) == 0 {
		return []*FileInfo{}, nil
	}

	header := records[0]
	drsURIIndex := -1
	imageURLIndex := -1
	nameIndex := -1
	for i, col := range header {
		switch col {
		case "drs_uri":
			drsURIIndex = i
		case "imageUrl":
			imageURLIndex = i
		case "name":
			nameIndex = i
		}
	}

	if drsURIIndex == -1 && imageURLIndex == -1 {
		return nil, fmt.Errorf("no 'drs_uri', 'imageUrl', 'SeriesInstanceUID', or 'Series UID' column found in %s", file.Name())
	}

	var fileInfos []*FileInfo
	for _, record := range records[1:] {
		var fileName string
		if nameIndex != -1 && len(record) > nameIndex {
			fileName = record[nameIndex]
		}

		if drsURIIndex != -1 {
			if len(record) > drsURIIndex {
				uri := record[drsURIIndex]
				if fileName == "" {
					fileName = filepath.Base(uri)
				}
				fileInfos = append(fileInfos, &FileInfo{
					DRSURI:    uri,
					SeriesUID: filepath.Base(uri),
					FileName:  fileName,
				})
			}
		} else {
			if len(record) > imageURLIndex {
				url := record[imageURLIndex]
				if fileName == "" {
					fileName = filepath.Base(url)
				}
				fileInfos = append(fileInfos, &FileInfo{
					DownloadURL: url,
					SeriesUID:   filepath.Base(url),
					FileName:    fileName,
				})
			}
		}
	}

	return fileInfos, nil
}

var ErrSeriesUIDColumnNotFound = fmt.Errorf("no 'SeriesInstanceUID' column found")

// getSeriesUIDsFromSpreadsheet extracts a list of SeriesInstanceUIDs from a spreadsheet
func getSeriesUIDsFromSpreadsheet(filePath string) ([]string, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	decoder, err := getSpreadsheetDecoder(filePath)
	if err != nil {
		return nil, err
	}

	records, err := decoder.Decode(file)
	if err != nil {
		return nil, err
	}

	if len(records) == 0 {
		return []string{}, nil
	}

	header := records[0]
	seriesInstanceUIDIndex := -1
	for i, col := range header {
		if col == "SeriesInstanceUID" || col == "Series UID" {
			seriesInstanceUIDIndex = i
			break
		}
	}

	if seriesInstanceUIDIndex == -1 {
		return nil, ErrSeriesUIDColumnNotFound
	}

	var seriesUIDs []string
	for _, record := range records[1:] {
		if len(record) > seriesInstanceUIDIndex {
			seriesUIDs = append(seriesUIDs, record[seriesInstanceUIDIndex])
		}
	}

	return seriesUIDs, nil
}