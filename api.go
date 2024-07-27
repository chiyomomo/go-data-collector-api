package events

import (
	"bytes"
	"encoding/json"
	"net/http"
	"sync"
	"time"
)

const DefaultEventLimit int = 100

// defines the payload structure of an event
type EventPayload map[string]interface{}

// represents the data structure for an event
type EventData struct {
	Type    string       `json:"type"`
	Payload EventPayload `json:"payload"`
}

// defines the options for the EventsApi
type EventOptions struct {
	URL         string
	OnSend      func(int)
	SendTimeout time.Duration
	Disabled    bool
}

// represents the structure for event handling
type EventsApi struct {
	waitingEvents []EventData
	options       EventOptions
	client        *http.Client
	sendCh        chan struct{}
	mutex         sync.Mutex
}

// creates a new instance of EventsApi
func NewEventsApi(opts EventOptions) *EventsApi {
	if opts.URL == "" {
		panic("URL must be specified")
	}
	if opts.SendTimeout == 0 {
		opts.SendTimeout = 2000 * time.Millisecond
	}
	if opts.OnSend == nil {
		opts.OnSend = func(count int) {}
	}

	api := &EventsApi{
		options: opts,
		client: &http.Client{
			Timeout: 5 * time.Second,
		},
		sendCh: make(chan struct{}, 1),
	}

	go api.debouncedSend()

	return api
}

// returns the current Unix time in seconds
func getCurrentUnixTime() int64 {
	return time.Now().UnixMilli()
}

// splits an EventData array into chunks of a specified size
func splitEventDataArrayIntoChunks(arr []EventData, chunkSize int) [][]EventData {
	var chunks [][]EventData
	for i := 0; i < len(arr); i += chunkSize {
		end := i + chunkSize

		// Ensure not to exceed the array bounds
		if end > len(arr) {
			end = len(arr)
		}

		chunks = append(chunks, arr[i:end])
	}
	return chunks
}

// handles the debouncing of event sending
func (api *EventsApi) debouncedSend() {
	for {
		<-api.sendCh
		time.Sleep(api.options.SendTimeout)
		api.mutex.Lock()
		events := api.waitingEvents
		api.waitingEvents = nil
		api.mutex.Unlock()

		if len(events) > 0 {
			for _, v := range splitEventDataArrayIntoChunks(events, DefaultEventLimit) {
				api.sendEvents(v)
			}
		}
	}
}

// sends the accumulated events
func (api *EventsApi) sendEvents(events []EventData) {
	payload, err := json.Marshal(events)
	if err != nil {
		// todo handle error
		return
	}

	_, err = api.client.Post(api.options.URL+"/event", "application/json", bytes.NewBuffer(payload))
	if err != nil {
		// todo handle error
		// todo handle 400 status
		return
	}

	api.options.OnSend(len(events))
}

// checks if the API is available
func (api *EventsApi) IsAvailable() bool {
	if api.options.Disabled {
		return false
	}
	resp, err := api.client.Get(api.options.URL + "/status")
	if err != nil {
		return false
	}

	return resp.StatusCode == http.StatusOK
}

// sends an event
func (api *EventsApi) SendEvent(eventType string, payload EventPayload) {
	if api.options.Disabled {
		return
	}

	if _, ok := payload["ts_local"]; !ok {
		payload["ts_local"] = getCurrentUnixTime()
	}

	event := EventData{
		Type:    eventType,
		Payload: payload,
	}

	api.mutex.Lock()
	api.waitingEvents = append(api.waitingEvents, event)
	api.mutex.Unlock()

	select {
	case api.sendCh <- struct{}{}:
	default:
	}
}
