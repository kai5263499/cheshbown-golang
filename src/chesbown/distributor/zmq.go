package distributor

import (
	"github.com/nu7hatch/gouuid"
	. "github.com/kai5263499/cheshbown-proto"
	zmq "github.com/pebbe/zmq4"
	"chesbown/worker"
	"time"
	"chesbown"
	"github.com/Sirupsen/logrus"
)

type ZmqDistributor struct {
	Actions
	chesbown.Zmq
}

func NewZmqDistributor() *ZmqDistributor {
	id, _ := uuid.NewV4()

	context, _ := zmq.NewContext()
	socket, _ := context.NewSocket(zmq.ROUTER)
	bind_addr := chesbown.GenerateSocketAddress(*id, NodeType_Distributor)
	logrus.Infof("%v binding to %v", id.String(), bind_addr)
	socket.SetLinger(time.Second * 10)
	socket.SetRcvhwm(1000)
	socket.SetSndhwm(1000)
	err := socket.Bind(bind_addr)
	if err != nil {
		logrus.Errorf("unable to create local distributor socket %v", err)
	}

	known_nodes := map[uuid.UUID]*Worker{
		*id: &Worker{
			Id: []byte(id.String()),
			NodeType: NodeType_Distributor,
			LastSeen: time.Now().Unix(),
			Status: NodeStatus_ACTIVE,
		},
	}

	other_sockets := make(map[uuid.UUID]*zmq.Socket)

	return &ZmqDistributor{
		Zmq: chesbown.Zmq{
			Metadata: chesbown.Metadata{
				Id: *id,
				KnownNodes: known_nodes,
			},

			Context: context,
			LocalSocket: socket,
			OtherSockets: other_sockets,
		},
	}
}

func (distributor *ZmqDistributor) NewWorker(node_type NodeType) *worker.ZmqWorker {
	return worker.NewZmqWorkerInitialize(node_type, distributor.KnownNodes, distributor.Context)
}

func (distributor *ZmqDistributor) Start() {
	logrus.Infof("type=%v id=%v starting", chesbown.NodeTypeToString(NodeType_Distributor), distributor.Id.String())

	distributor.ConnectToKnownNodes()

	distributor.Gossip()

	distributor.StartSocketReaders()

	distributor.GossipTimer()
}
