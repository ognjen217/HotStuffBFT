package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/ognjen217/HotStuffBFT/internal/scenario"
)

func main() {
	name := flag.String("scenario", "happy", "scenario to run: happy, silent-leader, byzantine-equivocation, byzantine-forged-qc, banking-block-transfer, delayed-network")
	n := flag.Int("n", 4, "number of replicas")
	f := flag.Int("f", 1, "maximum Byzantine replicas")
	timeoutMS := flag.Int("timeout-ms", 150, "initial view timeout in milliseconds; the pacemaker backs off exponentially")
	seed := flag.Int64("seed", 1, "deterministic random seed")
	verbose := flag.Bool("verbose", false, "print network-level trace entries")
	logDir := flag.String("log-dir", "logs", "directory where terminal log output is saved; use an empty value to disable file logging")
	vizDir := flag.String("viz-dir", "visualizations", "directory where HTML visualizations are written")
	visualize := flag.Bool("visualize", true, "after saving the log, call scripts/visualize_log.py and create an HTML visualization")
	flag.Parse()

	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
	defer cancel()

	result, err := scenario.Run(ctx, scenario.Config{
		Name:    *name,
		N:       *n,
		F:       *f,
		Timeout: time.Duration(*timeoutMS) * time.Millisecond,
		Seed:    *seed,
		Verbose: *verbose,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}

	summary := result.Summary()
	fmt.Print(summary)

	if strings.TrimSpace(*logDir) == "" {
		return
	}

	logPath, err := writeLogFile(*logDir, *name, summary)
	if err != nil {
		fmt.Fprintf(os.Stderr, "warning: could not save log file: %v\n", err)
		return
	}
	fmt.Fprintf(os.Stderr, "\nSaved log: %s\n", logPath)

	if !*visualize {
		return
	}

	vizPath, err := visualizeLog(logPath, *vizDir, *name)
	if err != nil {
		fmt.Fprintf(os.Stderr, "warning: could not create visualization: %v\n", err)
		fmt.Fprintf(os.Stderr, "manual command: python3 scripts/visualize_log.py --log %s --out %s\n", logPath, filepath.Join(*vizDir, safeFileName(*name)+".html"))
		return
	}
	fmt.Fprintf(os.Stderr, "Saved visualization: %s\n", vizPath)
}

func writeLogFile(dir, scenarioName, content string) (string, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", err
	}
	path := filepath.Join(dir, safeFileName(scenarioName)+".txt")
	return path, os.WriteFile(path, []byte(content), 0o644)
}

func visualizeLog(logPath, vizDir, scenarioName string) (string, error) {
	if err := os.MkdirAll(vizDir, 0o755); err != nil {
		return "", err
	}
	outPath := filepath.Join(vizDir, safeFileName(scenarioName)+".html")
	args := []string{"scripts/visualize_log.py", "--log", logPath, "--out", outPath}

	if err := runPython("python3", args...); err != nil {
		if fallbackErr := runPython("python", args...); fallbackErr != nil {
			return "", fmt.Errorf("python3 failed: %w; python failed: %v", err, fallbackErr)
		}
	}
	return outPath, nil
}

func runPython(binary string, args ...string) error {
	cmd := exec.Command(binary, args...)
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

func safeFileName(value string) string {
	value = strings.TrimSpace(strings.ToLower(value))
	var b strings.Builder
	lastDash := false
	for _, r := range value {
		safe := (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '_' || r == '-'
		if safe {
			b.WriteRune(r)
			lastDash = r == '-'
			continue
		}
		if !lastDash {
			b.WriteByte('-')
			lastDash = true
		}
	}
	out := strings.Trim(b.String(), "-")
	if out == "" {
		return "scenario"
	}
	return out
}
