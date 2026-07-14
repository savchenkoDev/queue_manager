package result

import "fmt"

type Result struct {
	JobID string
	WorkerUUID string
	Success bool
	Error error
}

func NewResult(jobID string, workerUUID string, success bool, error error) Result {
	return Result{JobID: jobID, WorkerUUID: workerUUID, Success: success, Error: error}
}

func (r *Result) String() string {
	return fmt.Sprintf("Result{JobID: %s, WorkerUUID: %s, Success: %t, Error: %v}", r.JobID, r.WorkerUUID, r.Success, r.Error)
}