package worker

import (
	uuid "github.com/nu7hatch/gouuid"
	zmq "github.com/pebbe/zmq4"
	"github.com/golang/protobuf/proto"
	. "github.com/kai5263499/cheshbown-proto"
	"github.com/Sirupsen/logrus"
	"chesbown"
)

type SocketReader func(socket *zmq.Socket) func()

type ZmqWorker struct {
	Actions
	chesbown.Zmq
}

func NewZmqWorkerInitialize(worker_type NodeType, seeds map[uuid.UUID]*Worker, context *zmq.Context) *ZmqWorker {

	id, _ := uuid.NewV4()

	socket, _ := context.NewSocket(zmq.ROUTER)
	bind_addr := chesbown.GenerateSocketAddress(*id, worker_type)
	logrus.Infof("%v binding to %v", id.String(), bind_addr)
	err := socket.Bind(bind_addr)
	if err != nil {
		logrus.Errorf("unable to create local distributor socket %v", err)
	}

	other_sockets := make(map[uuid.UUID]*zmq.Socket)

	zmq_worker := &ZmqWorker{
		Zmq: chesbown.Zmq{
			Metadata: chesbown.Metadata{
				Id: *id,
				NodeType: worker_type,
				KnownNodes: seeds,
			},
			Context: context,
			LocalSocket: socket,
			//Reactor: reactor,
			OtherSockets: other_sockets,
		},
	}

	return zmq_worker
}

func (worker *ZmqWorker) ReadMsg(sock *zmq.Socket) {
	for {
		data, err := sock.RecvBytes(zmq.DONTWAIT)
		if err != nil {
			break
		}

		msg := &Base{}
		if err := proto.Unmarshal(data, msg); err != nil {
			logrus.Error("unable to unmarshal protobuf from zmq")
		}

		logrus.Debug("received %s", msg)
	}
}

func (worker *ZmqWorker) ReadLoop() {
	for {
		//now := time.Now()

		msg, err := worker.LocalSocket.RecvMessage(0)
		if err != nil && len(msg) > 0 {
			logrus.Infof("msg: %s", msg)
		}

		//worker.reactor.AddSocket(worker.thread_sockets[SUBSCRIBE], zmq.POLLIN, func(e zmq.State) (err error) {
		//	worker.ReadMsg(worker.thread_sockets[SUBSCRIBE])
		//	return
		//})
		//worker.reactor.AddSocket(worker.thread_sockets[THREAD], zmq.POLLIN, func(e zmq.State) (err error) {
		//	worker.ReadMsg(worker.thread_sockets[THREAD])
		//	return
		//})
		//worker.reactor.AddChannelTime(time.Tick(time.Second), 1, func(i interface{}) (err error) {
		//	worker.Gossip()
		//	return
		//})

		//if worker.last_gossip.Add(time.Duration(1) * time.Minute).Before(now) {
		//	worker.Gossip()
		//}
	}
}



func (worker *ZmqWorker) Start() {
	logrus.Infof("type=%v id=%v starting", chesbown.NodeTypeToString(worker.NodeType), worker.Id.String())

	worker.ConnectToKnownNodes()

	worker.StartSocketReaders()

	// Introduce ourselves
	worker.Gossip()

	worker.GossipTimer()
}

