package scheduler

import (
	"context"
	"errors"
	"testing"
	"time"

	"manager/internal/job"
	"manager/internal/queue"
	"manager/internal/result"
)


func TestSchedulerSendToJobsChannel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	q := queue.NewQueue("test")
	jobs := make(chan *job.Job, 1)
	results := make(chan result.Result, 1)
	scheduler := New(&q, jobs, results)

	j := job.NewJob(map[string]any{"id": 1}, job.Normal, 3)
	q.Push(j)
	
	go scheduler.Run(ctx)
	
	select {
	case got := <-jobs:
		if got.UUID != j.UUID {
			t.Fatalf("Got wrong job: got = %v, want %v", got.UUID, j.UUID)
		}
		if !q.IsEmpty() {
			t.Fatalf("Queue is not empty: got = %v, want %v", q.Size(), 0)
		}
	case <-time.After(time.Second):
		t.Fatal("scheduler didn't dispatch job")
	}
}

func TestSchedulerCompleteJob(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	q := queue.NewQueue("test")
	jobs := make(chan *job.Job, 1)
	results := make(chan result.Result, 1)
	scheduler := New(&q, jobs, results)

	j := job.NewJob(map[string]any{"id": 1}, job.Normal, 3)
	q.Push(j)
	done := make(chan struct{})
	go func() {
		scheduler.Run(ctx)
		close(done)
	}()

	<-jobs // wait for job to be dispatched
	results <- result.Result{JobUUID: j.UUID, Error: nil}

	select {
	case <-done:
		if scheduler.runningJobsCount() != 0 {
			t.Fatalf("Running jobs count is wrong: got = %v, want %v", scheduler.runningJobsCount(), 0)
		}
		if scheduler.statistics.CompletedJobs != 1 {
			t.Fatalf("Completed jobs count is wrong: got = %v, want %v", scheduler.statistics.CompletedJobs, 1)
		}
	case <-time.After(time.Second):
		t.Fatal("scheduler didn't complete job")
	}
}


func TestSchedulerRetryJob(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	q := queue.NewQueue("test")
	jobs := make(chan *job.Job, 1)
	results := make(chan result.Result, 1)
	s := New(&q, jobs, results)

	j := job.NewJob(map[string]any{"id": 1}, job.Normal, 2)
	q.Push(j) // push job to queue
	
	go s.Run(ctx)
	<-jobs // wait for job to be dispatched
	results <- result.Result{JobUUID: j.UUID, Error: errors.New("test error")} // not nil
	got := <-jobs // get job from channel (retry appointment)
	if got.UUID != j.UUID {
		t.Fatalf("Got wrong job: got = %v, want %v", got.UUID, j.UUID)
	}
	if got.Attempts != 1 {
		t.Fatalf("Attempts count is wrong: got = %v, want %v", got.Attempts, 1)
	}
	if s.statistics.CompletedJobs != 0 {
		t.Fatalf("Completed jobs count is wrong: got = %v, want %v", s.statistics.CompletedJobs, 0)
	}
	if s.statistics.FailedJobs != 0 {
		t.Fatalf("Failed jobs count is wrong: got = %v, want %v", s.statistics.FailedJobs, 0)
	}
	cancel()
}

func TestSchedulerFailJob(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	q := queue.NewQueue("test")
	jobs := make(chan *job.Job, 1)
	results := make(chan result.Result, 1)
	s := New(&q, jobs, results)
	done := make(chan struct{})

	j := job.NewJob(map[string]any{"id": 1}, job.Normal, 2)
	q.Push(j) // push job to queue
	
	go func() {
		s.Run(ctx)
		close(done)
	}()
	r := result.Result{JobUUID: j.UUID, Error: errors.New("test error")}
	<-jobs // dispatch job
	results <- r // Attempts=1 → retry
	<-jobs // dispatch job after retry
	results <- r // Attempts=2 → fail
	

	select {
	case <-done:
		if s.statistics.CompletedJobs != 0 {
			t.Fatalf("Completed jobs count is wrong: got = %v, want %v", s.statistics.CompletedJobs, 0)
		}
		if s.statistics.FailedJobs != 1 {
			t.Fatalf("Failed jobs count is wrong: got = %v, want %v", s.statistics.FailedJobs, 1)
		}
		if s.runningJobsCount() != 0 {
			t.Fatalf("Running jobs count is wrong: got = %v, want %v", s.runningJobsCount(), 0)
		}
		if !q.IsEmpty() {
			t.Fatalf("Queue is not empty: got = %v, want %v", q.Size(), 0)
		}
	case <-time.After(time.Second):
		t.Fatal("scheduler didn't fail job")
	}	
}