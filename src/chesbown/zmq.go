package chesbown

import (
	"github.com/nu7hatch/gouuid"
	"time"
	zmq "github.com/pebbe/zmq4"
	"github.com/Sirupsen/logrus"
	. "github.com/kai5263499/cheshbown-proto"
	"github.com/gogo/protobuf/proto"
	//"bytes"
)

type Actions interface {
	HandleMsg(*Base)
}

type Zmq struct {
	Metadata
	Actions
	Context      *zmq.Context
	OtherSockets map[uuid.UUID]*zmq.Socket
	LocalSocket  *zmq.Socket
	Reactor      *zmq.Reactor
	LastGossip   *time.Time
}

func (node *Zmq) Connect(node_id uuid.UUID) {
	if node_id == node.Id {
		return
	}

	socket, _ := node.Context.NewSocket(zmq.DEALER)
	socket.SetIdentity(node.Id.String())
	known_peer := node.KnownNodes[node_id]

	socket_address := GenerateSocketAddress(node_id, known_peer.NodeType)

	logrus.Infof("%v connecting to %v via %v", node.Id.String(), node_id.String(), socket_address)

	err := socket.Connect(socket_address)
	if err != nil {
		logrus.Errorf("error connecting to socket for node %v, err=%v", node_id.String(), err)
	}

	node.OtherSockets[node_id] = socket
}

func (node *Zmq) NewBaseMessage(msg_type MessageType) *Base {
	base := Base{
		Sender: []byte(node.Id.String()),
		NodeType: node.NodeType,
		Timestamp: time.Now().Unix(),
		MessageType: msg_type,
	}
	return &base
}

func (node *Zmq) Gossip() {
	msg := node.NewBaseMessage(MessageType_GOSSIP)
	//workers := make([]*Worker, len(node.KnownNodes))
	//
	//for peer_id := range node.KnownNodes {
	//	workers = append(workers, node.KnownNodes[peer_id])
	//}
	//
	//msg.Workers = workers

	logrus.Infof("gossip msg=%v knownnodes=%v len=%v", msg, node.KnownNodes, len(node.KnownNodes))

	if len(node.KnownNodes) > 100 {
		// TODO: Pick a random subset to send
		logrus.Errorf("oops, too many known peers")
	} else {
		for peer := range node.KnownNodes {
			if peer != node.Id {
				node.Send(msg, peer)
			}
		}
	}
}

func (node *Zmq) Send(base *Base, to uuid.UUID) {
	out, err := proto.Marshal(base)
	if err != nil {
		logrus.Errorf("Failed to encode message %v with error %v", base, err)
	}

	logrus.Infof("%v sending %v to %v", node.Id.String(), base, to.String())

	num, err := node.OtherSockets[to].Send(node.Id.String(), zmq.SNDMORE)
	if err != nil {
		logrus.Errorf("error sending msg. %v err=%v", num, err)
	}
	node.OtherSockets[to].SendBytes([]byte(to.String()), zmq.SNDMORE)
	node.OtherSockets[to].SendBytes(out, 0)
}

func (node *Zmq) ConnectToKnownNodes() {
	// Connect to known nodes
	for known_node_id := range node.KnownNodes {
		node.Connect(known_node_id)
	}
}

func (node *Zmq) ReadFromSocket(sock *zmq.Socket) {
	go func() {
		for {
			msg, err := sock.RecvMessage(0)
			logrus.Infof("readfromsocket %v %v", msg, err)

			if err != nil {
				logrus.Errorf("error receiving message %v", err)
				continue
			}
			base := &Base{}

			logrus.Infof("got msg %v", msg)

			sender_id, _ := uuid.Parse([]byte(msg[0]))

			logrus.Infof("sender_id=%v", sender_id)

			//  Request is in second frame of message
			//buf := new(bytes.Buffer)
			//buf.ReadString(msg[1])
			request := msg[0]

			err = proto.Unmarshal([]byte(request), base)
			if err != nil {
				logrus.Errorf("error unmarshalling %v err=%v", msg[1:], err)
				continue
			}

			logrus.Infof("unmarshalled %v from %v", base, sender_id.String())

			node.HandleMsg(base)
		}
	}()
}

func (node *Zmq) ReactorHandleGenerator(id uuid.UUID, socket *zmq.Socket) func(e zmq.State) (err error) {
	return func(zmq.State) (error) {
		msg, err := node.LocalSocket.RecvMessage(0)

		if err != nil {
			logrus.Debugf("error with handlereactor for %v socket err=%v", id.String(), err)
			return err
		}
		sender_id := msg[0]

		//  Request is in second frame of message
		request := msg[1]

		logrus.Infof("msg from %v %v", sender_id, request)

		return err
	}
}

func (node *Zmq) StartSocketReaders() {
	go node.ReadFromSocket(node.LocalSocket)

	for node_id := range node.KnownNodes {
		if node_id == node.Id {
			continue
		}

		logrus.Infof("addsocket for %v", node_id.String())
		go node.ReadFromSocket(node.OtherSockets[node_id])
	}
}

func (node *Zmq) GossipTimer() {
	go func() {
		for {
			timer := time.NewTimer(time.Second * 5)
			<-timer.C
			node.Gossip()
		}
	}()
}