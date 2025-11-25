package main

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/suyashkumar/dicom"
	"github.com/suyashkumar/dicom/pkg/tag"
)

type DicomFile struct {
	Path             string
	SeriesUID        string
	AcquisitionNumber int
	InstanceNumber   int
}

func ProcessDicomFile(filePath string) (*DicomFile, error) {
	dataset, err := dicom.ParseFile(filePath, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to parse DICOM file %s: %v", filePath, err)
	}

	seriesUID, err := getElementValue(dataset, tag.SeriesInstanceUID)
	if err != nil {
		return nil, err
	}
	acquisitionNumberStr, err := getElementValue(dataset, tag.AcquisitionNumber)
	if err != nil {
		// AcquisitionNumber is optional, so we can default to 0
		acquisitionNumberStr = "0"
	}
	instanceNumberStr, err := getElementValue(dataset, tag.InstanceNumber)
	if err != nil {
		return nil, err
	}

	acquisitionNumber, _ := strconv.Atoi(acquisitionNumberStr)
	instanceNumber, _ := strconv.Atoi(instanceNumberStr)

	return &DicomFile{
		Path:             filePath,
		SeriesUID:        seriesUID,
		AcquisitionNumber: acquisitionNumber,
		InstanceNumber:   instanceNumber,
	}, nil
}

func getElementValue(dataset dicom.Dataset, tag tag.Tag) (string, error) {
	element, err := dataset.FindElementByTag(tag)
	if err != nil {
		return "", fmt.Errorf("could not find tag %v", tag)
	}
	// Trim leading/trailing brackets and spaces
	return strings.Trim(element.Value.String(), "[] "), nil
}
