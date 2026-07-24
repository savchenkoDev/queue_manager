package processor

import (
	"errors"
	"math/rand"
	"time"

	"manager/internal/job"
	"manager/internal/result"
)

type Processor interface {
	Process(job *job.Job) result.Result
}

type RealProcessor struct {
}

func (p *RealProcessor) Process(job *job.Job) result.Result {
	time.Sleep(1 * time.Second)    // simulate job processing time
	success := rand.Intn(2)%2 == 0 // simulate job success/failure
	if success {
		return result.NewResult(job.UUID, nil)
	}
	return result.NewResult(job.UUID, errors.New("test error"))
}

// SpyProcessor is a processor that records the number of calls and the jobs it received
// It is used to test the processor
// It is not used in production
type SpyProcessor struct {
    Results      []result.Result
    Calls        int
    ReceivedJobs []*job.Job
}

func (p *SpyProcessor) Process(job *job.Job) result.Result {
    p.Calls++
    p.ReceivedJobs = append(p.ReceivedJobs, job)
    return p.Results[p.Calls-1]
}
