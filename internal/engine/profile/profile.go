package profile

import (
	"bytes"
	"fmt"
	"sort"
	"strings"
	"text/tabwriter"
	"time"
)

// Profile is a collection of profiling events that were collected by a
// profiler.
type Profile struct {
	Events []Event
}

func (p Profile) String() string {
	var buf bytes.Buffer
	w := tabwriter.NewWriter(&buf, 5, 4, 3, ' ', tabwriter.AlignRight)

	evts := p.Events
	sort.Slice(evts, func(i, j int) bool { return strings.Compare(evts[i].Name, evts[j].Name) < 0 })

	firstEvt, lastEvt := evts[0], evts[0]
	for _, evt := range evts {
		if evt.Start.Before(firstEvt.Start) {
			firstEvt = evt
		}
		if evt.Start.After(lastEvt.Start) {
			lastEvt = evt
		}
	}

	buckets := make(map[string][]Event)
	for _, evt := range evts {
		buckets[evt.Name] = append(buckets[evt.Name], evt)
	}

	_, _ = fmt.Fprintf(w, "%v\t\t%v\t%v\t%v\t%v\t\n", "event", "calls", "min", "avg", "max")
	for _, bucketEvts := range buckets {
		totalDuration := 0 * time.Second
		minBucketDur := bucketEvts[0].Duration
		maxBucketDur := bucketEvts[0].Duration
		for _, bucketEvt := range bucketEvts {
			totalDuration += bucketEvt.Duration
			if bucketEvt.Duration < minBucketDur {
				minBucketDur = bucketEvt.Duration
			}
			if bucketEvt.Duration > maxBucketDur {
				maxBucketDur = bucketEvt.Duration
			}
		}
		avgBucketDur := totalDuration / time.Duration(len(bucketEvts))
		_, _ = fmt.Fprintf(w, "%v\t\t%v\t%v\t%v\t%v\t\n", bucketEvts[0].Name, len(bucketEvts), minBucketDur, avgBucketDur, maxBucketDur)
	}

	_ = w.Flush()
	return buf.String()
}
