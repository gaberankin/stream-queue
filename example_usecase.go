package main

import (
	"encoding/json"
	"fmt"
)

type testJobData struct {
	Data string `json:"data"`
}

func worker(payload json.RawMessage) error {
	var d testJobData

	if err := json.Unmarshal(payload, &d); err != nil {
		return err
	}

	fmt.Printf("data: %s\n", d.Data)

	return nil
}
