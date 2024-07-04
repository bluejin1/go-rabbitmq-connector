package amqp_helper

import (
	"os"
	"runtime"
	"strconv"
)

func GetEnv(key string, defaultVal string) string {
	if val, ok := os.LookupEnv(key); ok {
		return val
	}

	return defaultVal
}

func GetEnvAsInt(key string, defaultVal int) int {
	strVal := GetEnv(key, "")

	if val, err := strconv.Atoi(strVal); err == nil {
		return val
	}
	return defaultVal
}

// AWS TLS
func GetEnvBool(key string, defaultVal bool) bool {
	if val, ok := os.LookupEnv(key); ok {
		boolVal, err := strconv.ParseBool(val)
		if err != nil {
			boolVal = false
		}
		return boolVal
	}

	return defaultVal
}

func GetEnvBoolFromStr(key string, defaultVal string) bool {
	keyVal := defaultVal
	if val, ok := os.LookupEnv(key); ok {
		keyVal = val
	}
	if keyVal == "true" {
		return true
	}
	return false
}

func GetMaxParallelism() int {
	maxProcs := runtime.GOMAXPROCS(0)
	numCPU := runtime.NumCPU()
	if maxProcs < numCPU {
		return maxProcs
	}
	return numCPU
}
