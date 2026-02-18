package main

import (
	"log/slog"
	"sync"
	"time"
)

func main() {
	slog.Info("Uptime monitor")

	stopAll := make(chan struct{})
	var wg sync.WaitGroup
	compTargets("targets.yaml", &wg, stopAll)
	wg.Wait()
}

type TargetProberAnalyser struct {
	target Target
	prober *Prober
	analyserStopC chan struct{} // only has stopChan for now
}

// Spawn new Prober or ask a Prober to stop
func compTargets(targetsFile string, wg *sync.WaitGroup, stopChan chan struct{}) {
	readTargetInterval := time.Duration(60) * time.Second
	probeTargetInterval := time.Duration(30) * time.Second
	probeTargetTimeout := time.Duration(5) * time.Second
	targetMap := map[string]TargetProberAnalyser{}
	watcher := NewWatcher(targetsFile, readTargetInterval, wg)

	// TODO:
	//  - use other things as unique key in map

	for {
		select {
		case targetList := <-watcher.targetsC:
			if len(targetList) == 0 {
				slog.Info("Target list is empty, either error or no targets")
				for _, tpa := range targetMap {
					tpa.prober.stopC <- struct{}{}
					tpa.analyserStopC <- struct{}{}
				}
			} else {
				// Temporary map used for removing targets later
				tempTargetMap := make(map[string]bool, len(targetList))

				// Add targets
				for _, t := range targetList {
					tempTargetMap[t.Endpoint] = true
					if tpa, exists := targetMap[t.Endpoint]; !exists { // Use endpoint for now, need to change in future
						slog.Info("Spawning new prober for new target", "endpoint", t.Endpoint)
						newProber := NewProber(t.Endpoint, probeTargetTimeout, probeTargetInterval, wg)
						analyserStopChan := make(chan struct{}, 1)

						wg.Add(1)
						go analyse(wg, newProber.probeResC, analyserStopChan)

						targetMap[t.Endpoint] = TargetProberAnalyser{
							target: t,
							prober: newProber,
							analyserStopC: analyserStopChan,
						}
					} else {
						// Still replace, because labels may change
						tpa.target = t
						targetMap[t.Endpoint] = tpa
					}
				}

				// Remove targets
				for key, tpa := range targetMap {
					if(!tempTargetMap[key]) {
						tpa.prober.stopC <- struct{}{}
						tpa.analyserStopC <- struct{}{}
						delete(targetMap, key)
					}
				}
			}
		case <-stopChan:
			for _, tpa := range targetMap {
				tpa.prober.stopC <- struct{}{}
				tpa.analyserStopC <- struct{}{}
			}	
			return
		}
	}
}

func analyse(wg *sync.WaitGroup, probeResChan chan ProbeResult, stopChan chan struct{}) {
	defer wg.Done()

	for {
		select {
		case res := <- probeResChan:
				if res.err != nil {
					slog.Error("Endpoint probing error", "error", res.err)
				} else {
					slog.Info("Endpoint probing successful", "endpoint", res.endpoint, "code", res.resp.StatusCode)
				}
		case <- stopChan:
			slog.Info("Shutting down analyser")
			return
		}
	}
}

