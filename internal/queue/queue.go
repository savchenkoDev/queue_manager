package queue

import (
	"errors"
	"manager/internal/structures"
)

type Queue struct {
	name string
	jobs []structures.Job
}

func NewQueue(name string) *Queue {
	return &Queue{
		name: name,
		jobs: []structures.Job{},
	}
}

func (q *Queue) Enqueue(job *structures.Job) error {
	q.jobs = append(q.jobs, *job)
	return nil
}

func (q *Queue) Pop() (*structures.Job, error) {
	if len(q.jobs) == 0 {
		return nil, errors.New("queue is empty")
	}
	job := q.jobs[0]
	q.jobs = q.jobs[1:len(q.jobs)]

	return &job, nil
}

func (q *Queue) Len() int {
	return len(q.jobs)
}

func (q *Queue) IsEmpty() bool {
	return len(q.jobs) == 0
}