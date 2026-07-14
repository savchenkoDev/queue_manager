package scheduler

import (
	"log"
	"manager/internal/job"
	"manager/internal/queue"
	"manager/internal/result"
)

type Scheduler struct {
	queue   *queue.Queue
	jobs    chan<- job.Job
	results <-chan result.Result
}

func New(queue *queue.Queue, jobs chan<- job.Job, results <-chan result.Result) *Scheduler {
	return &Scheduler{queue: queue, jobs: jobs, results: results}
}

func (s *Scheduler) DispatchJobs() {
	for {
		job, err := s.queue.Pop()
		if err != nil {
			break
		}
		s.jobs <- job
	}
	close(s.jobs)
}

func (s *Scheduler) HandleResults() {
	for result := range s.results {
		log.Println(result.String())
	}
}
