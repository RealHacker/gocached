package main

import "net"
import "fmt"
import "os"
import "runtime"

const (
	PROT = "tcp"
	HOST = "127.0.0.1"
	PORT = "3333"
)


func StartServing(){
	// start serving 
	l, err := net.Listen(PROT, HOST+":"+PORT)
	if err != nil {
		fmt.Println("Fail to start listening on port " + PORT)
		os.Exit(1)
	}
	defer l.Close()
	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Fail to accept a connection")
			continue
		}
		go ConnectionHandler(conn)
	}
}

func main(){
	runtime.GOMAXPROCS(runtime.NumCPU())
	StartServing()
}