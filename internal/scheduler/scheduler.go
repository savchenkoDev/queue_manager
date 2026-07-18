package scheduler

import (
	"sync"
	"log"
	"time"

	"manager/internal/types"
	"manager/internal/job"
	"manager/internal/queue"
	"manager/internal/result"
)

type Scheduler struct {
	queue   *queue.Queue
	runningJobs map[string]*job.Job
	jobs    chan<- *job.Job
	results <-chan result.Result
	mu          sync.Mutex
	statistics  Statistics
}

type Statistics struct {
	CompletedJobs int `json:"completed_jobs"`
	FailedJobs int `json:"failed_jobs"`
}

func New(queue *queue.Queue, jobs chan<- *job.Job, results <-chan result.Result) *Scheduler {
	return &Scheduler{
		queue: queue,
		runningJobs: make(map[string]*job.Job),
		jobs: jobs,
		results: results,
		mu: sync.Mutex{},
		statistics: Statistics{CompletedJobs: 0, FailedJobs: 0},
	}
}

func (s *Scheduler) DispatchJobs() {
	for {
		job, _ := s.queue.Pop()

		if job != nil {
			s.AddRunningJob(job)
			s.jobs <- job
		}
        
		s.mu.Lock()
		runningJobsCount := len(s.runningJobs)
		s.mu.Unlock()

		if s.queue.IsEmpty() && runningJobsCount == 0 {
			break
		}
	}
	close(s.jobs)
}

func (s *Scheduler) AddRunningJob(job *job.Job) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.runningJobs[job.UUID] = job
}

func (s *Scheduler) HandleResults() {
	for result := range s.results {
		s.mu.Lock()
		job, ok := s.runningJobs[result.JobUUID]
		if !ok {
			s.mu.Unlock()
			continue
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
		s.mu.Unlock()
		log.Println("[RESULT] Priority:", job.Priority, "ID:", result.JobUUID, "Attempts:", job.Attempts, "Status:", job.Status)
	}

	log.Println("[RESULT] Completed:", s.statistics.CompletedJobs, "Failed:", s.statistics.FailedJobs)
}

func (s *Scheduler) complete(job *job.Job) {
	job.Status = types.COMPLETED
	job.UpdatedAt = time.Now()
	delete(s.runningJobs, job.UUID)
	s.statistics.CompletedJobs++
}

func (s *Scheduler) retry(job *job.Job) {
	job.Status = types.PENDING
	job.UpdatedAt = time.Now()
	s.queue.Push(*job)
	delete(s.runningJobs, job.UUID)
}

func (s *Scheduler) fail(job *job.Job) {
	job.Status = types.FAILED
	job.UpdatedAt = time.Now()
	delete(s.runningJobs, job.UUID)
	s.statistics.FailedJobs++
}