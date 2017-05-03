package distributor

import (
	. "github.com/kai5263499/cheshbown-proto"
)

type Metadata struct {
}

type Actions interface {
	NewWorker(NodeType) *Worker
}

