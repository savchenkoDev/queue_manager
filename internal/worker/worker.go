package worker

import (
	"time"
	"math/rand"
	"errors"

	"manager/internal/job"
	"manager/internal/result"

	"github.com/google/uuid"
)

type Worker struct {
	UUID    string
	jobs    <-chan job.Job
	results chan<- result.Result
}

func NewWorker(jobs <-chan job.Job, results chan<- result.Result) *Worker {
	return &Worker{UUID: uuid.New().String(), jobs: jobs, results: results}
}

func Run(jobs <-chan *job.Job, results chan<- result.Result) {
	for job := range jobs {
		result := process(job)
		results <- result
	}
}

func process(job *job.Job) result.Result {
	time.Sleep(1 * time.Second) // simulate job processing time
	success := rand.Intn(2) % 2 == 0 // simulate job success/failure
	if success {
		return result.NewResult(job.UUID, nil)
	}
	return result.NewResult(job.UUID, errors.New("test error"))
}

