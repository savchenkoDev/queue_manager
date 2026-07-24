package worker

import (
	"context"
	"testing"
	"time"

	"manager/internal/job"
	"manager/internal/processor"
	"manager/internal/result"
)

func TestWorkerProcessesJob(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	jobs := make(chan *job.Job, 1)
	results := make(chan result.Result, 1)
	j := job.NewJob(map[string]any{"id": 1}, job.Normal, 3)
	processor := &processor.SpyProcessor{
		Result: result.NewResult(j.UUID, nil),
	}
	worker := NewWorker(ctx, processor)
	go worker.Run(jobs, results)
    jobs <- &j
	res := <-results
	cancel()
	if res.JobUUID != j.UUID {
		t.Fatalf("result job UUID = %q, want %q", res.JobUUID, j.UUID)
	}
	if processor.Calls != 1 {
    	t.Fatalf("processor calls = %d, want 1", processor.Calls)
	}
	if processor.ReceivedJobs[0] != &j {
		t.Fatalf("processor received job = %+v, want %+v", processor.ReceivedJobs[0], &j)
	}
}	

func TestWorkerProcessMuptipleJobs(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	jobsChan := make(chan *job.Job, 3)
	resultsChan := make(chan result.Result, 3)
	jobs := []job.Job{
		job.NewJob(map[string]any{"id": 1}, job.Normal, 3),
		job.NewJob(map[string]any{"id": 2}, job.Normal, 3),
		job.NewJob(map[string]any{"id": 3}, job.Normal, 3),
	}
	processor := &processor.SpyProcessor{}
	worker := NewWorker(ctx, processor)
	go worker.Run(jobsChan, resultsChan)
	for j := 0; j < len(jobs); j++ {
		jobsChan <- &jobs[j]
	}
	for i := 0; i < len(jobs); i++ {
		<-resultsChan
	}
	if processor.Calls != len(jobs) {
		t.Errorf("processor should have been called %d times", len(jobs))
	}
	for i, j := range processor.ReceivedJobs {
		if j.UUID != jobs[i].UUID {
			t.Errorf("processor should have received the job %d", i)
		}
	}
	cancel()
}

func TestWorkerStopsWhenJobsClosed(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan struct{})

	jobs := make(chan *job.Job)
	results := make(chan result.Result)

	worker := NewWorker(ctx, &processor.SpyProcessor{})

	go func() {
		worker.Run(jobs, results)
		close(done)
	}()

	close(jobs)

	select {
	case <-done:

	case <-time.After(time.Second):
		t.Fatal("worker did not stop")
	}
}

func TestWorkerStopsOnContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})

	jobs := make(chan *job.Job)
	results := make(chan result.Result)

	worker := NewWorker(ctx, &processor.SpyProcessor{})

	go func() {
		worker.Run(jobs, results)
		close(done)
	}()

	cancel()

	select {
	case <-done:

	case <-time.After(time.Second):
		t.Fatal("worker did not stop after cancel")
	}
}

func TestWorkerContextIsDoneDuringJobIsProcessed(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	jobs := make(chan *job.Job, 1)
	results := make(chan result.Result, 1)
	processor := &processor.SpyProcessor{}
	worker := NewWorker(ctx, processor)
	go worker.Run(jobs, results)
	jobs <- &job.Job{UUID: "1"}
	close(jobs)
	<-results
	cancel()
	if processor.Calls != 1 {
		t.Errorf("processor should have been called %d times", processor.Calls)
	}
	if processor.ReceivedJobs[0].UUID != "1" {
		t.Errorf("processor should have received the job with UUID %q", "1")
	}
}