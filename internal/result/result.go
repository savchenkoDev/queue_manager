package result

import "fmt"

type Result struct {
	JobUUID string
	Error   error
}

func NewResult(jobUUID string, error error) Result {
	return Result{JobUUID: jobUUID, Error: error}
}

func (r *Result) String() string {
	return fmt.Sprintf("Result{JobUUID: %s, Error: %v}", r.JobUUID, r.Error)
}
