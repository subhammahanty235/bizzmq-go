package bizzmq

import "time"

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
	}
}
func (m *Message) UpdateLifeCycleStatus(newStatus string) *Message {
	validStates := []string{"waiting", "processing", "processed", "failed", "requeued"}
	isValid := false
	for _, status := range validStates {
		if status == newStatus {
			isValid = true
			break
		}
	}

	if !isValid {
		return nil
	}
	m.Status = newStatus
	m.TimestampUpdated = time.Now().UnixMilli()
	return m
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
	}
}
