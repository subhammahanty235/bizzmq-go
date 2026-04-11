package bizzmq

import (
	"fmt"
	"time"
)

/*
		this.message_id = message_id
        this.queue_name = queue_name;
        this.message = message;
        this.status = 'waiting' //default lifecycle state
        this.timestamp_created = new Date()
        this.timestamp_updated = this.timestamp_created;
        this.options = {
            priority: options.priority || 0,
            retries: options.retries || 1,

        }
        this.retries_made = 0;

*/

type MessageOptions struct {
	Priority int64 `json:"priority"`
	Retries  int64 `json:"retries"`
}

type Message struct {
	QueueName        string         `json:"queue_name"`
	MessageID        string         `json:"message_id"`
	Message          interface{}    `json:"message"`
	Options          MessageOptions `json:"options"`
	TimestampCreated int64          `json:"timestamp_created"`
	TimestampUpdated int64          `json:"timestamp_updated"`
	Status           string         `json:"status"`
	RetriesMade      int            `json:"retries_made"`
}

func NewMessage(queueName string, messageID string, message interface{}, options MessageOptions) *Message {
	return &Message{
		QueueName:        queueName,
		MessageID:        messageID,
		Message:          message,
		Options:          options,
		TimestampCreated: time.Now().UnixMilli(),
		TimestampUpdated: time.Now().UnixMilli(),
		Status:           "waiting",
		RetriesMade:      0,
	}
}
func (m *Message) UpdateLifeCycleStatus(newStatus string) error {
	validStates := []string{"waiting", "processing", "processed", "failed", "requeued"}
	for _, status := range validStates {
		if status == newStatus {
			m.Status = newStatus
			m.TimestampUpdated = time.Now().UnixMilli()
			return nil
		}
	}
	return fmt.Errorf("invalid status %q: must be one of %v", newStatus, validStates)
}

func (m *Message) ToJSON() map[string]interface{} {
	return map[string]interface{}{
		"queue_name":        m.QueueName,
		"message_id":        m.MessageID,
		"message":           m.Message,
		"options":           m.Options,
		"timestamp_created": m.TimestampCreated,
		"timestamp_updated": m.TimestampUpdated,
		"status":            m.Status,
		"retries_made":      m.RetriesMade,
	}
}
