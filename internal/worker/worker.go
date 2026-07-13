package worker

import (
	"log"
	"time"

	"manager/internal/structures"
	"manager/internal/result"
	
	"github.com/google/uuid"
)

type Worker struct {
	UUID string
}

func NewWorker() *Worker {
	return &Worker{UUID: uuid.New().String()}
}

func (w *Worker) Process(job structures.Job) *result.Result {
	log.Println("Processing job: ", job.UUID)
	time.Sleep(1 * time.Second)

	result := result.NewResult(job.UUID, true, nil)
	return result
}