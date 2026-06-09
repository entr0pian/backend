package worker

import (
	"encoding/json"
	"log"

	"taskapp/backend/internal/model"
)

func LogWorker(logQueue chan model.TaskEvent) {
	for event := range logQueue {
		message, _ := json.Marshal(event)
		log.Print(string(message))
	}
}
