package common

import (
	"fmt"
	"strconv"
	"strings"
)

// Validate endpoint (IP:port or DomainName:port)
func IsValidEndpoint(endpoint string) bool {
	parts := strings.Split(endpoint, ":")
	if len(parts) != 2 {
		return false
	}
	port := parts[1]
	if _, err := strconv.Atoi(port); err != nil {
		return false
	}
	return true
}

// BlockName format: fileName_blockID_versionID
func ConstructBlockName(fileName string, blockID uint, versionID uint) string {
	return fmt.Sprintf("%s_%d_%d", fileName, blockID, versionID)
}

// Parse blockName and returns fileName, blockID, versionID, and potential error
func ParseBlockName(blockName string) (string, uint, uint, error) {
	parts := strings.Split(blockName, "_")
	// the last part is the versionID
	versionID, err := strconv.Atoi(parts[len(parts)-1])
	if err != nil {
		return "", 0, 0, err
	}
	// the second last part is the blockID
	blockID, err := strconv.Atoi(parts[len(parts)-2])
	if err != nil {
		return "", 0, 0, err
	}
	// the other parts is the fileName
	fileName := strings.Join(parts[:len(parts)-2], "_")
	return fileName, uint(blockID), uint(versionID), nil
}
