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
	Decode(file *os.File) ([]string, error)
}

// CSVDecoder decodes CSV files
type CSVDecoder struct{}

// TSVDecoder decodes TSV files
type TSVDecoder struct{}

// XLSXDecoder decodes XLSX files
type XLSXDecoder struct{}

// Decode decodes a CSV file and returns the values from the "imageUrl" column
func (d *CSVDecoder) Decode(file *os.File) ([]string, error) {
	return decodesv(file, ',')
}

// Decode decodes a TSV file and returns the values from the "imageUrl" column
func (d *TSVDecoder) Decode(file *os.File) ([]string, error) {
	return decodesv(file, '\t')
}

// decodesv decodes a separated value file and returns the values from the "imageUrl" column
func decodesv(file *os.File, separator rune) ([]string, error) {
	reader := csv.NewReader(file)
	reader.Comma = separator
	records, err := reader.ReadAll()
	if err != nil {
		return nil, err
	}

	if len(records) == 0 {
		return []string{}, nil
	}

	header := records[0]
	imageURLIndex := -1
	for i, col := range header {
		if col == "imageUrl" {
			imageURLIndex = i
			break
		}
	}

	if imageURLIndex == -1 {
		return nil, fmt.Errorf("no 'imageUrl' column found in %s", file.Name())
	}

	var imageURLs []string
	for _, record := range records[1:] {
		if len(record) > imageURLIndex {
			imageURLs = append(imageURLs, record[imageURLIndex])
		}
	}
	return imageURLs, nil
}

// Decode decodes an XLSX file and returns the values from the "imageUrl" column
func (d *XLSXDecoder) Decode(file *os.File) ([]string, error) {
	xlFile, err := xlsx.OpenReaderAt(file, -1)
	if err != nil {
		return nil, err
	}

	var imageURLs []string
	for _, sheet := range xlFile.Sheets {
		if len(sheet.Rows) == 0 {
			continue
		}

		header := sheet.Rows[0]
		imageURLIndex := -1
		for i, cell := range header.Cells {
			if cell.String() == "imageUrl" {
				imageURLIndex = i
				break
			}
		}

		if imageURLIndex == -1 {
			continue
		}

		for _, row := range sheet.Rows[1:] {
			if len(row.Cells) > imageURLIndex {
				imageURLs = append(imageURLs, row.Cells[imageURLIndex].String())
			}
		}
	}

	if len(imageURLs) == 0 {
		return nil, fmt.Errorf("no 'imageUrl' column found in any sheet of %s", file.Name())
	}

	return imageURLs, nil
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

	imageURLs, err := decoder.Decode(file)
	if err != nil {
		return nil, err
	}

	var fileInfos []*FileInfo
	for _, url := range imageURLs {
		fileInfos = append(fileInfos, &FileInfo{
			DownloadURL: url,
			SeriesUID:   filepath.Base(url),
		})
	}

	return fileInfos, nil
}