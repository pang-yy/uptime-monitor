package main

import (
	"fmt"
	"net/http"
	"sync"
	"time"
)

type ProbeResult struct {
	endpoint  	string
	resp 		*http.Response
	err  		error
}

type Prober struct {
	probeResC chan ProbeResult
	stopC chan struct{}
}

// Need: endpoint, timeout, where to send result to, waitgroup
func NewProber(endpoint string, timeout time.Duration, interval time.Duration, wg *sync.WaitGroup) *Prober {
	probeResChan := make(chan ProbeResult, 3)
	stopChan := make(chan struct{}, 1)
	prober := &Prober{
		probeResC: probeResChan,
		stopC: stopChan,
	}

	wg.Add(1)
	go func() {
		defer wg.Done()

		ticker := time.NewTicker(interval)
		client := newClient(timeout)
		for {
			select {
			case <-ticker.C:
				prober.probeResC <- probe(client, endpoint)
			case <-prober.stopC:
				return
			}
		}
	}()

	return prober
}

func newClient(timeout time.Duration) (*http.Client) {
	return &http.Client{
		Timeout: timeout,
	}
}

func probe(customClient *http.Client, url string) ProbeResult {
	fmt.Println("probing ", url)
	r, err := customClient.Get(url)
	return ProbeResult{
		endpoint: url,
		resp: r,
		err: err,
	}
}
