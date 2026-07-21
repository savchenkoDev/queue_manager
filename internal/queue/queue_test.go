package queue

import (
	"testing"
	"manager/internal/job"
)

func TestQueueIsEmpty(t *testing.T) {
	tests := []struct {
		name string
		jobs []job.Job
		expected bool
	}{
		{
			name: "EmptyQueue",
			jobs: []job.Job{},
			expected: true,
		},
		{
			name: "NonEmptyQueue",
			jobs: []job.Job{job.NewJob(map[string]any{"id": 1}, job.Normal, 3)},
			expected: false,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			queue := NewQueue("test")
			for _, job := range test.jobs {
				queue.Push(job)
			}
			if queue.IsEmpty() != test.expected {
				t.Fatalf("expected %v, got %v", test.expected, queue.IsEmpty())
			}
		})
	}
}

func TestQueuePop(t *testing.T) {
	highJob := job.NewJob(map[string]any{"id": 1}, job.High, 3)
	normalJob1 := job.NewJob(map[string]any{"id": 2}, job.Normal, 3)
	normalJob2 := job.NewJob(map[string]any{"id": 3}, job.Normal, 3)
	normalJob3 := job.NewJob(map[string]any{"id": 4}, job.Normal, 3)
	lowJob := job.NewJob(map[string]any{"id": 5}, job.Low, 3)

	tests := []struct {
		name string
		jobs []job.Job
		expected []job.Job
		wantErr bool
	}{
		{
			name: "DifferentPriority",
			jobs: []job.Job{highJob, normalJob1, lowJob},
			expected: []job.Job{highJob, normalJob1, lowJob},
		},
		{
			name: "SamePriority",
			jobs: []job.Job{normalJob3, normalJob2, normalJob1},
			expected: []job.Job{normalJob3, normalJob2, normalJob1},
		},
		{
			name: "EmptyQueue",
			jobs: []job.Job{},
			expected: []job.Job{},
			wantErr: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			queue := NewQueue("test")
			for _, job := range test.jobs {
				queue.Push(job)
			}

			if test.wantErr {
				_, err := queue.Pop()
				if err == nil {
					t.Fatal("expected error")
				}
				return
			}
			for i, expected := range test.expected {
				popped, err := queue.Pop()
				if err != nil {
					t.Fatalf("expected no error, got %v", err)
				}
				if popped.UUID != expected.UUID {
					t.Fatalf("pop #%d: expected %s, got %s", i, expected.UUID, popped.UUID )
				}
			}
		})
	}
}