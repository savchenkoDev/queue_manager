package job

import (
	"time"
	"manager/internal/types"
)

type Job struct {
	UUID string `json:"uuid"`
	Type string `json:"type"`
	Status types.Status `json:"status" default:"pending"`
	Payload any `json:"payload"`
	CreatedAt time.Time `json:"created_at" time_format:"2006-01-02 15:04:05"`
	UpdatedAt time.Time `json:"updated_at" time_format:"2006-01-02 15:04:05"`
}