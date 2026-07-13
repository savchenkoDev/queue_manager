package internal

import (
	"manager/internal/result"
	"manager/internal/structures"
)

type Processor interface {
    Process(job structures.Job) *result.Result
}