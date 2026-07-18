package queue

import (
	"errors"
	"sync"
	"sort"

	"manager/internal/job"
)

type Queue struct {
	name string
	jobs []job.Job
	mu sync.Mutex
}

func NewQueue(name string) Queue {
	return Queue{
		name: name,
		jobs: []job.Job{},
		mu: sync.Mutex{},
	}
}

func (q *Queue) Push(job job.Job) error {
	defer q.mu.Unlock()
	q.mu.Lock()
	q.jobs = append(q.jobs, job)
	q.sortByPriority()
	return nil
}

func (q *Queue) Pop() (*job.Job, error) {
	defer q.mu.Unlock()

	q.mu.Lock()
	if len(q.jobs) == 0 {
		return nil, errors.New("queue is empty")
	}
	job := q.jobs[0]
	q.jobs = q.jobs[1:len(q.jobs)]

	return &job, nil
}

func (q *Queue) IsEmpty() bool {
	defer q.mu.Unlock()
	
	q.mu.Lock()
	isEmpty := len(q.jobs) == 0
	return isEmpty
}

func (q *Queue) sortByPriority() {
	sort.Slice(q.jobs, func(i, j int) bool {
		return q.jobs[i].Priority > q.jobs[j].Priority
	})
}
