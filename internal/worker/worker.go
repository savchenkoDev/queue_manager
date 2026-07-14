package worker

import (
	"time"

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

func (w *Worker) Process(job job.Job) result.Result {
	time.Sleep(1 * time.Second)             // simulate job processing time
	success := time.Now().UnixNano()%2 == 0 // simulate job success/failure
	return result.NewResult(job.UUID, w.UUID, success, nil)
}

func (w *Worker) Run() {
	for job := range w.jobs {
		result := w.Process(job)
		w.results <- result
	}
}
