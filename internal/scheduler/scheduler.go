package scheduler

import (
	"context"
	"log"
	"time"

	"manager/internal/job"
	"manager/internal/queue"
	"manager/internal/result"
	"manager/internal/types"
)

type Scheduler struct {
	queue       *queue.Queue
	runningJobs map[string]*job.Job
	jobs        chan<- *job.Job
	results     <-chan result.Result
	statistics  Statistics
}

type Statistics struct {
	CompletedJobs int `json:"completed_jobs"`
	FailedJobs    int `json:"failed_jobs"`
}

func New(queue *queue.Queue, jobs chan<- *job.Job, results <-chan result.Result) *Scheduler {
	return &Scheduler{
		queue:       queue,
		runningJobs: make(map[string]*job.Job),
		jobs:        jobs,
		results:     results,
		statistics:  Statistics{CompletedJobs: 0, FailedJobs: 0},
	}
}

func (s *Scheduler) Run(ctx context.Context) {
	for {
		nextJob, err := s.queue.Peek()
		if err != nil {
			if s.runningJobsCount() == 0 {
				close(s.jobs)
				return
			}
			select {
			case result := <-s.results:
				s.handleResult(result)
			case <-ctx.Done():
				close(s.jobs)
				return
			}
			continue
		}

		select {
		case result := <-s.results:
			s.handleResult(result)
		case s.jobs <- nextJob:
			s.queue.Remove(nextJob.UUID)
			s.addRunningJob(nextJob)
		case <-ctx.Done():
			close(s.jobs)
			return
		}
	}
}

func (s *Scheduler) runningJobsCount() int {
	return len(s.runningJobs)
}

func (s *Scheduler) getRunningJob(uuid string) (*job.Job, bool) {
	job, ok := s.runningJobs[uuid]
	return job, ok
}

func (s *Scheduler) addRunningJob(job *job.Job) {
	s.runningJobs[job.UUID] = job
}

func (s *Scheduler) deleteRunningJob(uuid string) {
	delete(s.runningJobs, uuid)
}

func (s *Scheduler) handleResult(result result.Result) {
	job, ok := s.getRunningJob(result.JobUUID)
	if !ok {
		return
	}

	job.Attempts++

	switch {
	case result.Error == nil:
		s.complete(job)
	case job.Attempts < job.MaxRetries:
		s.retry(job)
	default:
		s.fail(job)
	}
	log.Println("[RESULT] Priority:", job.Priority, "ID:", result.JobUUID, "Attempts:", job.Attempts, "Status:", job.Status)
}

func (s *Scheduler) complete(job *job.Job) {
	job.Status = types.COMPLETED
	job.UpdatedAt = time.Now()
	s.deleteRunningJob(job.UUID)
	s.statistics.CompletedJobs++
}

func (s *Scheduler) retry(job *job.Job) {
	job.Status = types.PENDING
	job.UpdatedAt = time.Now()
	s.queue.Push(*job)
	s.deleteRunningJob(job.UUID)
}

func (s *Scheduler) fail(job *job.Job) {
	job.Status = types.FAILED
	job.UpdatedAt = time.Now()
	s.deleteRunningJob(job.UUID)
	s.statistics.FailedJobs++
}
