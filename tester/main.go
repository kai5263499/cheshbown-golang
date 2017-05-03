package main

import (
	. "github.com/kai5263499/cheshbown-proto"
	"github.com/Sirupsen/logrus"
	"chesbown/distributor"
	"os"
	"os/signal"
	"syscall"
	"fmt"
	//"time"
)

func main() {
	logrus.Info("Chesbown tester starting up")

	distributor := distributor.NewZmqDistributor()
	distributor.Start()

	//time.Sleep(time.Second * 5)

	worker := distributor.NewWorker(NodeType_Lender)
	worker.Start()

	sigs := make(chan os.Signal, 1)
	done := make(chan bool, 1)

	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigs
		fmt.Println()
		fmt.Println(sig)
		done <- true
	}()

	fmt.Println("awaiting signal")
	<-done
	fmt.Println("exiting")
}
