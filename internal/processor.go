package internal

import (
	"manager/internal/job"
	"manager/internal/result"
)

type Processor interface {
	Process(job job.Job) *result.Result
}
