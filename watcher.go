package main

import (
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/goccy/go-yaml"
)

type Watcher struct {
	targetsC chan []Target
	stopC chan struct{}
}

type Target struct {
	Endpoint string				`yaml:"url"`
	Labels map[string]string	`yaml:"labels"`
}

type Targets struct {
	TargetList []Target `yaml:"targets"`
}

func NewWatcher(filename string, interval time.Duration, wg *sync.WaitGroup) *Watcher {
	absFilename, err := filepath.Abs(filepath.Clean(filename))
	if err != nil {
		slog.Error("Failed to resolve filename", "file", err)
		return nil
	}

	stopChan := make(chan struct{}, 1)
	resChan := make(chan []Target, 3)
	watcher := &Watcher{
		targetsC: resChan,
		stopC: stopChan,
	}
	targets, lastModTime, err := reloadTargetsIfChange(absFilename, time.Time{})
	if err != nil {
		slog.Error("Failed to reload target list file", "error", err)
		resChan <- []Target{}
	} else {
		resChan <- targets
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		var targets []Target
		var lastModTime time.Time = lastModTime
		var newLastModTime time.Time
		ticker := time.NewTicker(interval)
		for {
			select {
			case <-ticker.C:
				targets, newLastModTime, err = reloadTargetsIfChange(absFilename, lastModTime)

				// If error, pass empty Target list
				// If no new update, do nothing
				// If has new update, pass new Target list
				if err != nil {
					slog.Error("Failed to reload target list file", "error", err)
					resChan <- []Target{}
				} else if newLastModTime.After(lastModTime) {
					lastModTime = newLastModTime
					resChan <- targets
				}
			case <-watcher.stopC:
				return
			}
		}
	}()

	return watcher
}

func reloadTargetsIfChange(filename string, lastModTime time.Time) ([]Target, time.Time, error) {
	slog.Info("Reloading target list file", "file", filename)
	info, err := os.Stat(filename)
	if err != nil {
		slog.Error("Failed to obtain stats of target list file", "file", filename)
		return nil, lastModTime, err
	}

	if !info.ModTime().After(lastModTime) {
		slog.Info("Target list file unchanged", "file", filename)
		return nil, lastModTime, nil
	}

	f, err := os.Open(filename)
	if err != nil {
		slog.Error("Failed to open target list file", "file", filename)
		return nil, lastModTime, err
	}
	defer f.Close()

	var targets Targets
	if err := yaml.NewDecoder(f).Decode(&targets); err != nil {
		slog.Error("Failed to parse target list file", "file", filename)
		return nil, lastModTime, err
	}

	slog.Info("Succesfully parse target list file", "file", filename)
	return targets.TargetList, info.ModTime(), nil
}
