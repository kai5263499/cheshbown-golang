package chesbown

import (
	"github.com/nu7hatch/gouuid"
	. "github.com/kai5263499/cheshbown-proto"
	"fmt"
)

type Metadata struct {
	Id uuid.UUID
	KnownNodes map[uuid.UUID]*Worker
	NodeType   NodeType
}

const SocketAddressPrefix = "inproc://chesbown"

func NodeTypeToString(node_type NodeType) string {
	switch node_type {
	case NodeType_Distributor:
		return "distributor"
	case NodeType_LenderSupervisor:
		return "lender_supervisor"
	case NodeType_Lender:
		return "lender"
	case NodeType_BorrowerSupervisor:
		return "borrower_supervisor"
	case NodeType_Borrower:
		return "borrower"
	default:
		panic("this isn't good")
	}
}

func GenerateSocketAddress(id uuid.UUID, node_type NodeType) string {
	return fmt.Sprintf("%s_%s_%s", SocketAddressPrefix, NodeTypeToString(node_type), id.String())
}