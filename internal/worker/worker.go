package worker

import (
	"context"
	"manager/internal/job"
	"manager/internal/processor"
	"manager/internal/result"
)

type Worker struct {
	ctx context.Context
	processor processor.Processor
}

func NewWorker(ctx context.Context, processor processor.Processor) *Worker {
	return &Worker{ctx: ctx, processor: processor}
}

func (w *Worker) Run(jobs <-chan *job.Job, results chan<- result.Result) {
	for {
		select {
		case <-w.ctx.Done():
			return
		case job, ok := <-jobs:
			if !ok {
				return
			}
			result := w.processor.Process(job)
			results <- result
		}
	}
}
