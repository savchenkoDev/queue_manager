package result

import "fmt"

type Result struct {
	JobID string
	Success bool
	Error error
}

func NewResult(jobID string, success bool, error error) *Result {
	return &Result{JobID: jobID, Success: success, Error: error}
}

func (r *Result) String() string {
	return fmt.Sprintf("Result{JobID: %s, Success: %t, Error: %v}", r.JobID, r.Success, r.Error)
}