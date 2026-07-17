package types

type Status string

const (
    PENDING Status = "pending"
    COMPLETED Status = "completed"
    FAILED Status = "failed"
)