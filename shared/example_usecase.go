package shared

import (
	"encoding/json"
	"fmt"
)

type TestJobData struct {
	Data string `json:"data"`
}

func Worker(payload json.RawMessage) error {
	var d TestJobData

	if err := json.Unmarshal(payload, &d); err != nil {
		return err
	}

	fmt.Printf("data: %s\n", d.Data)

	return nil
}
