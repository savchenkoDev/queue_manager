package types

type Status string

const (
    PENDING Status = "pending"
    RUNNING Status = "running"
    COMPLETED Status = "completed"
    FAILED Status = "failed"
)