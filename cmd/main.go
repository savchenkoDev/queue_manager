package main

import (
	"manager/internal/job"
	"manager/internal/queue"
	"manager/internal/result"
	"manager/internal/scheduler"
	"manager/internal/worker"
	"sync"

	"github.com/google/uuid"
)

const (
	workersCount = 3
	jobsCount    = 30
)

func main() {
	var appWG sync.WaitGroup
	var workerWG sync.WaitGroup
	jobs := make(chan job.Job)
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

func startScheduler(wg *sync.WaitGroup, queue *queue.Queue, jobs chan<- job.Job, results <-chan result.Result) {
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
		queue.Enqueue(job.Job{
			UUID:   uuid.New().String(),
			Status: "pending",
			Payload: map[string]interface{}{
				"id": i,
			},
		})
	}
}

func createWorkers(workerWG *sync.WaitGroup, jobs <-chan job.Job, results chan<- result.Result) {
	workerWG.Add(workersCount)
	for i := 0; i < workersCount; i++ {
		worker := worker.NewWorker(jobs, results)
		go func() {
			defer workerWG.Done()

			worker.Run()
		}()
	}
}
