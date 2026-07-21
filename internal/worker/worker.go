package worker

import (
	"manager/internal/job"
	"manager/internal/processor"
	"manager/internal/result"
)

type Worker struct {
	processor processor.Processor
}

func NewWorker(processor processor.Processor) *Worker {
	return &Worker{processor: processor}
}

func (w *Worker) Run(jobs <-chan *job.Job, results chan<- result.Result) {
	for job := range jobs {
		result := w.processor.Process(job)
		results <- result
	}
}
