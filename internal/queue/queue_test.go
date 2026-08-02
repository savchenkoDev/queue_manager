package queue

import (
	"testing"

	"manager/internal/job"
)

func TestQueueIsEmpty(t *testing.T) {
	tests := []struct {
		name     string
		jobs     []job.Job
		expected bool
	}{
		{
			name:     "EmptyQueue",
			jobs:     []job.Job{},
			expected: true,
		},
		{
			name:     "NonEmptyQueue",
			jobs:     []job.Job{job.NewJob(map[string]any{"id": 1}, job.Normal, 3)},
			expected: false,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			q := NewQueue("test")
			for _, j := range test.jobs {
				q.Push(j)
			}
			if q.IsEmpty() != test.expected {
				t.Fatalf("IsEmpty = %v, want %v", q.IsEmpty(), test.expected)
			}
		})
	}
}

func TestQueueSize(t *testing.T) {
	q := NewQueue("test")
	if q.Size() != 0 {
		t.Fatalf("Size = %v, want %v", q.Size(), 1)
	}
	job := job.NewJob(map[string]any{"id": 1}, job.Normal, 3)
	q.Push(job)
	if q.Size() != 1 {
		t.Fatalf("Size = %v, want %v", q.Size(), 1)
	}
	q.Remove(job.UUID)
	if q.Size() != 0 {
		t.Fatalf("Size = %v, want %v", q.Size(), 0)
	}
}

func TestQueuePeekDontRemoveJob(t *testing.T) {
	j := job.NewJob(map[string]any{"id": 1}, job.Normal, 3)
	q := NewQueue("test")
	q.Push(j)
	got, err := q.Peek()
	if err != nil {
		t.Fatalf("unexpected error: Peek = %v", err)
	}
	if got.UUID != j.UUID {
		t.Fatalf("got.UUID = %v, want %v", got.UUID, j.UUID)
	}
	got2, err := q.Peek()
	if err != nil {
		t.Fatalf("unexpected error: Peek = %v", err)
	}
	if got2.UUID != j.UUID {
		t.Fatalf("got2.UUID = %v, want %v", got2.UUID, j.UUID)
	}
	if q.IsEmpty() {
		t.Fatalf("q.IsEmpty() = %v, want %v", q.IsEmpty(), false)
	}
}

func TestQueuePeek(t *testing.T) {
	normalJob := job.NewJob(map[string]any{"id": 1}, job.Normal, 3)
	highJob := job.NewJob(map[string]any{"id": 2}, job.High, 2)
	lowJob := job.NewJob(map[string]any{"id": 3}, job.Low, 1)
	tests := []struct {
		name     string
		jobs     []job.Job
		want     job.Job
		wantErr bool
	}{
		{
			name: "EmptyQueue",
			jobs: []job.Job{},
			wantErr: true,
		},
		{
			name: "NonEmptyQueue",
			jobs: []job.Job{normalJob},
			want: normalJob,
			wantErr: false,
		},
		{
			name: "GotHighPriorityJob",
			jobs: []job.Job{normalJob, highJob, lowJob},
			want: highJob,
			wantErr: false,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			q := NewQueue("test")
			for _, j := range test.jobs {
				q.Push(j)
			}
			
			got, err := q.Peek()
			if (err != nil) != test.wantErr {
				t.Fatalf("Peek = %v, wantErr %v", err, test.wantErr)
			}
			if !test.wantErr && got.UUID != test.want.UUID {
				t.Fatalf("Peek = %v, want %v", got.UUID, test.want.UUID)
			}
		})
	}
}

func TestQueueRemoveJob(t *testing.T) {
	highJob := job.NewJob(map[string]any{"id": 3}, job.High, 1)
	normalJob := job.NewJob(map[string]any{"id": 2}, job.Normal, 1)
	lowJob := job.NewJob(map[string]any{"id": 4}, job.Low, 1)
	q := NewQueue("test")

	tests := []struct{
		name string
		jobs []job.Job
		uuid string
		wantErr bool
		wantMessage string
	}{
		{
			name: "EmptyQueue",
			jobs: []job.Job{},
			uuid: highJob.UUID,
			wantErr: true,
			wantMessage: "queue is empty",
		},
		{
			name: "NonEmptyQueue",
			jobs: []job.Job{highJob, normalJob, lowJob},
			uuid: highJob.UUID,
			wantErr: false,
		},
		{
			name: "JobNotFound",
			jobs: []job.Job{normalJob, lowJob},
			uuid: highJob.UUID,
			wantErr: true,
			wantMessage: "job not found",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			for _, j := range test.jobs {
				q.Push(j)
			}
			err := q.Remove(test.uuid)
			if err != nil && !test.wantErr {
				if err.Error() != test.wantMessage {
					t.Fatalf("Remove = %v, want %v", err, test.wantMessage)
				}
			}
		})
	}
}

func TestQueuePush(t *testing.T) {
	highJob := job.NewJob(map[string]any{"id": 3}, job.High, 1)
	normalJob1 := job.NewJob(map[string]any{"id": 2}, job.Normal, 1)
	normalJob2 := job.NewJob(map[string]any{"id": 1}, job.Normal, 1)
	tests := []struct{
		name string
		jobs []job.Job
		want int
	}{
		{
			name: "PushOneJob",
			jobs: []job.Job{normalJob1},
			want: 1,
		},
		{
			name: "PushMultipleJobs",
			jobs: []job.Job{highJob, normalJob1, normalJob2},
			want: 3,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			q := NewQueue("test")
			for _, j := range test.jobs {
				err := q.Push(j)
				if err != nil {
					t.Fatalf("Unexpected error: Push = %v", err)
				}
			}
			if q.Size() != test.want {
				t.Fatalf("Size = %v, want %v", q.Size(), test.want)
			}
		})
	}
}
