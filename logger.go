package main

import (
	"fmt"
	"sync"
)

var (
	simLogs []string
	logMu   sync.RWMutex
)

func AddLog(format string, args ...interface{}) {
	message := fmt.Sprintf(format, args...)
	fmt.Print(message)

	logMu.Lock()
	simLogs = append(simLogs, message)
	logMu.Unlock()
}

func ClearLogs() {
	logMu.Lock()
	simLogs = nil
	logMu.Unlock()
}

func GetLogs() []string {
	logMu.RLock()
	defer logMu.RUnlock()
	return append([]string(nil), simLogs...)
}
