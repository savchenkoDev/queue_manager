package queue

import (
	"errors"
	"sort"
	"sync"

	"manager/internal/job"
)

type Queue struct {
	name string
	jobs []job.Job
	mu   sync.Mutex
}

func NewQueue(name string) Queue {
	return Queue{
		name: name,
		jobs: []job.Job{},
		mu:   sync.Mutex{},
	}
}

func (q *Queue) Push(job job.Job) error {
	defer q.mu.Unlock()
	q.mu.Lock()
	q.jobs = append(q.jobs, job)
	q.sortByPriority()
	return nil
}

func (q *Queue) Peek() (*job.Job, error) {
	if q.IsEmpty() {
		return nil, errors.New("queue is empty")
	}
	return &q.jobs[0], nil
}

func (q *Queue) Remove(uuid string) error {
	if q.IsEmpty() {
		return errors.New("queue is empty")
	}
	defer q.mu.Unlock()
	q.mu.Lock()
	
	for i, j := range q.jobs {
		if j.UUID == uuid {
			q.jobs = append(q.jobs[:i], q.jobs[i+1:]...)
			return nil
		}
	}
	return errors.New("job not found")
}

func (q *Queue) Size() int {
	defer q.mu.Unlock()
	q.mu.Lock()
	return len(q.jobs)
}

func (q *Queue) IsEmpty() bool {
	return q.Size() == 0
}

func (q *Queue) sortByPriority() {
	sort.Slice(q.jobs, func(i, j int) bool {
		return q.jobs[i].Priority > q.jobs[j].Priority
	})
}
