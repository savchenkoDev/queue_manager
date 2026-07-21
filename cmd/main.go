package main

import (
	"context"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"manager/internal/job"
	"manager/internal/processor"
	"manager/internal/queue"
	"manager/internal/result"
	"manager/internal/scheduler"
	"manager/internal/worker"
)

const (
	workersCount = 3
	jobsCount    = 100
)

func main() {
	var appWG sync.WaitGroup
	var workerWG sync.WaitGroup
	jobs := make(chan *job.Job)
	results := make(chan result.Result)
	queue := queue.NewQueue("default")
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	seedQueue(&queue)
	createWorkers(&workerWG, jobs, results)
	startScheduler(ctx, &appWG, &queue, jobs, results)

	appWG.Add(1)
	go func() {
		defer appWG.Done()

		workerWG.Wait()
		close(results)
	}()

	appWG.Wait()
	stop()
}

func seedQueue(queue *queue.Queue) {
	priorities := []job.Priority{job.Low, job.Normal, job.High}
	for i := 0; i < jobsCount; i++ {
		queue.Push(job.NewJob(map[string]any{"id": i}, priorities[i%len(priorities)], 3))
	}
}

func createWorkers(workerWG *sync.WaitGroup, jobs <-chan *job.Job, results chan<- result.Result) {
	workerWG.Add(workersCount)
	processor := processor.RealProcessor{}
	for i := 0; i < workersCount; i++ {
		go func() {
			defer workerWG.Done()

			worker := worker.NewWorker(&processor)

			worker.Run(jobs, results)
		}()
	}
}

func startScheduler(ctx context.Context, wg *sync.WaitGroup, queue *queue.Queue, jobs chan<- *job.Job, results <-chan result.Result) {
	wg.Add(2)
	scheduler := scheduler.New(queue, jobs, results)

	go func() {
		defer wg.Done()

		scheduler.DispatchJobs(ctx)
	}()
	go func() {
		defer wg.Done()

		scheduler.HandleResults()
	}()
}
