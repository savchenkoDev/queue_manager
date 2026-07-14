package queue

import (
	"errors"
	"manager/internal/job"
)

type Queue struct {
	name string
	jobs []job.Job
}

func NewQueue(name string) Queue {
	return Queue{
		name: name,
		jobs: []job.Job{},
	}
}

func (q *Queue) Enqueue(job job.Job) error {
	q.jobs = append(q.jobs, job)
	return nil
}

func (q *Queue) Pop() (job.Job, error) {
	if len(q.jobs) == 0 {
		return job.Job{}, errors.New("queue is empty")
	}
	job := q.jobs[0]
	q.jobs = q.jobs[1:len(q.jobs)]

	return job, nil
}

func (q *Queue) Len() int {
	return len(q.jobs)
}

func (q *Queue) IsEmpty() bool {
	return len(q.jobs) == 0
}
