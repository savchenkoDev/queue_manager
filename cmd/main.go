package main

import (
	"manager/internal/scheduler"
	"manager/internal/worker"
	"manager/internal/queue"
	"manager/internal/structures"
	"github.com/google/uuid"
	"time"
)

func main() {
	// _ = godotenv.Load()

	// port := os.Getenv("APP_PORT")

	// if port == "" {
	// 	log.Fatalf("APP_PORT is not set in the environment variables")
	// }

	// rdb, err := config.NewRedis()
	// if err != nil {
	// 	log.Fatalf("failed to connect to redis: %v", err)
	// }
    // defer rdb.Close()

	// server := server.NewServer(rdb, port)
	// if err := server.Start(); err != nil {
	// 	log.Fatalf("failed to start server: %v", err)
	// }

	queue := queue.NewQueue("default")
	jobs := make([]structures.Job, 10)
	for i := 0; i < 10; i++ {
		jobs[i] = structures.Job{
			UUID: uuid.New().String(),
			Status: "pending",
			Payload: map[string]interface{}{
				"id": i,
			},
			CreatedAt: time.Now(),
		}
	}
	for _, job := range jobs {
		queue.Enqueue(&job)
	}
	processor := worker.NewWorker()
	scheduler := scheduler.NewScheduler(queue, processor)
	scheduler.Start()
}