package common

import (
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