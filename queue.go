//
// Propolis: Amazon S3 <--> local file system synchronizer
// Copyright © 2011 Russ Ross <russ@russross.com>
//
// This file is part of Propolis
//
// Propolis is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 2 of the License, or
// (at your option) any later version.
// 
// Propolis is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
// 
// You should have received a copy of the GNU General Public License
// along with Propolis.  If not, see <http://www.gnu.org/licenses/>.
//

// Update task queue manager

package main

import (
	"container/heap"
	"container/vector"
	"fmt"
	"os"
	"time"
)

type Candidate struct {
	Name     string
	Inserted int64
	Updated  int64
	Push     bool
}

type Queue struct {
	vector.Vector
}

func (q *Queue) Less(i, j int) bool {
	return q.At(i).(*Candidate).Inserted < q.At(j).(*Candidate).Inserted
}

type FileName struct {
	Name      string
	Immediate bool
	Push      bool
}

// Start the main queue loop. The channel that is returned
// accepts relative path names as input. It waits for at least
// delay seconds from the last time that path came through
// the channel, then issues a FileUpdate action on it.
// At most maxInFlight updates will be launched in parallel, which
// may delay some requests beyond delay seconds.
func StartQueue(p *Propolis, delay int, maxInFlight int) (check chan FileName, quit chan chan bool) {
	// a path coming in on this channel should be checked after a delay
	check = make(chan FileName)

	// the main queue of files that are waiting to be updated
	queue := new(Queue)

	// map of path -> candidate in the queue, useful for
	// finding existing entries
	pendingCandidates := make(map[string]*Candidate)

	// this channel triggers a check for an old-enough entry to update
	timeout := make(chan bool)

	// this channel indicates an update is complete
	finished := make(chan bool)

	// this channel tells the function to quit next time the queue is empty
	quit = make(chan chan bool)
	var shutdown chan bool

	// this indicates whether or not a worker is preparing to signal timeout
	waiting := false

	// count of how many updates are in progress
	inflight := 0

	go func() {
		for {
			select {
			case fn := <-check:
				path := fn.Name
				push := fn.Push
				immediate := fn.Immediate
				//fmt.Printf("Q: incoming request [%s]\n", path)

				// record the incoming request
				now := time.Nanoseconds()

				// are we already watching this file?
				if elt, present := pendingCandidates[path]; present {
					// touch an existing entry
					elt.Updated = now
					elt.Push = push
					//fmt.Printf("Q: pending candidate touched [%s]\n", path)
				} else {
					// new entry
					elt := &Candidate{path, now, now, push}
					if immediate {
						// move this request back in time
						elt.Inserted -= int64(delay) * 1e9
						elt.Updated -= int64(delay) * 1e9
					}

					// put it in the queue
					heap.Push(queue, elt)

					// and in the map so we can find it by path name
					pendingCandidates[path] = elt
					//fmt.Printf("Q: new candidate added [%s]\n", path)
				}

			case <-timeout:
				//fmt.Printf("Q: timeout expired, checking queue\n")
				waiting = false
				now := time.Nanoseconds()

				// check the head of the queue
				for queue.Len() > 0 {
					elt := heap.Pop(queue).(*Candidate)

					// was this updated while it waited?
					if elt.Inserted != elt.Updated {
						elt.Inserted = elt.Updated
						heap.Push(queue, elt)
						//fmt.Printf("Q: touched candidate requeued [%s]\n", elt.Name)
						continue
					}

					// has the delay been long enough?
					if now-elt.Inserted < int64(delay)*1e9 && shutdown == nil {
						heap.Push(queue, elt)
						//fmt.Printf("Q: oldest entry not old enough [%s]\n", elt.Name)
						break
					}

					// is there room for an update right now?
					if inflight < maxInFlight {
						inflight++
						pendingCandidates[elt.Name] = nil, false
						//fmt.Printf("Q: starting update [%s]\n", elt.Name)
						go func(path string, push bool) {
							// perform the actual update
							err := p.SyncFile(p.NewFile(path, push))
							if err != nil {
								fmt.Fprintf(os.Stderr, "Error updating [%s]: %v\n", path, err)
							}

							// signal that this update is finished
							// so another can begin
							finished <- true
						}(elt.Name, elt.Push)
					} else {
						heap.Push(queue, elt)
						//fmt.Printf("Q: too many updates in flight [%s]\n", elt.Name)
						break
					}
				}
				if queue.Len() == 0 {
					//fmt.Printf("Q: queue empty\n")
				}

			case <-finished:
				// a single update finished
				//fmt.Printf("Q: update finished\n")
				inflight--
				if inflight == 0 {
					//fmt.Printf("Q: no more requests in flight\n")
				}

			case shutdown = <-quit:
				// don't bother waiting for the pending sleeper thread (if any)
				waiting = false
				// shutdown != nil signals intent to shutdown
			}

			// launch a sleeper if necessary
			if !waiting && inflight < maxInFlight && queue.Len() > 0 {
				now := time.Nanoseconds()
				waiting = true
				headofqueue := queue.At(0).(*Candidate).Inserted
				howlong := headofqueue + int64(delay)*1e9 - now
				//fmt.Printf("Q: launching sleeper for %.2f seconds\n", float64(howlong)/1e9)
				go func(pause int64) {
					if pause > 0 && shutdown == nil {
						time.Sleep(pause)
					}
					//fmt.Printf("Q: sleeper finished\n")
					timeout <- true
				}(howlong)
			}

			if shutdown != nil && inflight == 0 && queue.Len() == 0 {
				shutdown <- true
				return
			}
		}
	}()
	return
}
