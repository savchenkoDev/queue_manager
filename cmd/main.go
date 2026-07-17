package main

import (
	"time"
	"sync"

	"manager/internal/job"
	"manager/internal/queue"
	"manager/internal/types"
	"manager/internal/result"
	"manager/internal/scheduler"
	"manager/internal/worker"

	"github.com/google/uuid"
)

const (
	workersCount = 3
	jobsCount    = 10
)

func main() {
	var appWG sync.WaitGroup
	var workerWG sync.WaitGroup
	jobs := make(chan *job.Job)
	results := make(chan result.Result)
	queue := queue.NewQueue("default")

	seedQueue(&queue)
	createWorkers(&workerWG, jobs, results)
	startScheduler(&appWG, &queue, jobs, results)
	
	appWG.Add(1)
	go func() {
		defer appWG.Done()

		workerWG.Wait()
		close(results)
	}()

	appWG.Wait()
}

func startScheduler(wg *sync.WaitGroup, queue *queue.Queue, jobs chan<- *job.Job, results <-chan result.Result) {
	wg.Add(2)
	scheduler := scheduler.New(queue, jobs, results)
	
	go func() {
		defer wg.Done()

		scheduler.DispatchJobs()
	}()
	go func() {
		defer wg.Done()

		scheduler.HandleResults()
	}()
}

func seedQueue(queue *queue.Queue) {
	for i := 0; i < jobsCount; i++ {
		queue.Push(job.Job{
			UUID:   uuid.New().String(),
			Status: types.PENDING,
			Attempts: 0,
			MaxRetries: 3,
			Payload: map[string]any{
				"id": i,
			},
			CreatedAt: time.Now(),
			UpdatedAt: time.Now(),
		})
	}
}

func createWorkers(workerWG *sync.WaitGroup, jobs <-chan *job.Job, results chan<- result.Result) {
	workerWG.Add(workersCount)
	for i := 0; i < workersCount; i++ {
		go func() {
			defer workerWG.Done()

			worker.Run(jobs, results)
		}()
	}
}
