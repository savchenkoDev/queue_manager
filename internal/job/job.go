package job

import (
	"time"
	"manager/internal/types"

	"github.com/google/uuid"
)

type Priority int

const (
    Low Priority = iota
    Normal
    High
)

type Job struct {
	UUID string `json:"uuid"`
	Type string `json:"type"`
	Status types.Status `json:"status" default:"pending"`
	Priority Priority `json:"priority" default:"Normal"`
	Attempts int `default:"0"`
	MaxRetries int `default:"3"`
	Payload any `json:"payload"`
	CreatedAt time.Time `json:"created_at" time_format:"2006-01-02 15:04:05"`
	UpdatedAt time.Time `json:"updated_at" time_format:"2006-01-02 15:04:05"`
}

func NewJob(payload any, priority Priority, maxRetries int) Job {
	return Job{
		UUID:   uuid.New().String(),
		Status: types.PENDING,
		Attempts: 0,
		MaxRetries: maxRetries,
		Payload: payload,
		Priority: priority,
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}
}
