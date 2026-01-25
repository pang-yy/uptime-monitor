package main

import (
	"fmt"
	"sync"
	"time"
)

func main() {
	fmt.Println("Started...")

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
	//	- implement ways to remove targets
	//  - use other things as unique key in map

	for {
		select {
		case targetList := <-watcher.targetsC:
			if len(targetList) == 0 {
				fmt.Println("Got empty list, either error or no targets")
			} else {
				for _, t := range targetList {
					if tpa, exists := targetMap[t.Endpoint]; !exists { // Use endpoint for now, need to change in future
						fmt.Println("trying to spawn new prober")
						newProber := NewProber(t.Endpoint, probeTargetTimeout, probeTargetInterval, wg)
						analyserStopChan := make(chan struct{}, 1)
						go analyse(wg, newProber.probeResC, analyserStopChan)
						targetMap[t.Endpoint] = TargetProberAnalyser{
							target: t,
							prober: newProber,
							analyserStopC: analyserStopChan,
						}
					} else {
						// Still replace, because labels may change
						targetMap[t.Endpoint] = TargetProberAnalyser{
							target: t,
							prober: tpa.prober,
							analyserStopC: tpa.analyserStopC,
						}
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
					fmt.Println(res.endpoint, ": ", res.err)
				} else {
					fmt.Println(res.endpoint, ": ", res.resp.StatusCode)
				}
		case <- stopChan:
			return
		}
	}
}

