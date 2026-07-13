package scheduler

import (
	"log"

	"manager/internal"
	"manager/internal/queue"
	"manager/internal/types"
)

type Scheduler struct {
	queue *queue.Queue
	worker internal.Processor
}


func NewScheduler(queue *queue.Queue, processor internal.Processor) *Scheduler {
	return &Scheduler{queue: queue, worker: processor}
}

func (s *Scheduler) Start() {
	for {
		job, err := s.queue.Pop()
		if err != nil {
			log.Println(err)
			break
		}
		result := s.worker.Process(*job)
		if result.Success {
			job.Status = types.Status(types.COMPLETED)
		} else {
			job.Status = types.Status(types.FAILED)
		}
		log.Println(result.String())
	}
}