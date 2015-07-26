package main

import "net"
import "fmt"
import "os"
import _ "strconv"
import "bytes"
import "bufio"

const (
	PROT = "tcp"
	HOST = "127.0.0.1"
	PORT = "3333"
)

var response string = "OK"

func respondAndCloseConn(conn net.Conn){
	conn.Write(([]byte)(response))
	if err := conn.Close(); err != nil {
		fmt.Println("Fail to close connection")
	}
}
func ConnectionHandler(conn net.Conn){
	defer respondAndCloseConn(conn)

	reader := bufio.NewReader(conn)
	splitter := []byte("\r")
	firstline, err := reader.ReadBytes(splitter[0])
	if err != nil {
		fmt.Println("Not enough chars in command")
		return
	}

	index := bytes.Index(firstline, splitter)

	commandline := string(firstline[:index])
	body, err := reader.ReadBytes(splitter[0])
	body = body[1:len(body)-1] 	//escape \n

	// protocol parsing
	result, err := HandleClientRequest(commandline, &body)
	fmt.Println(fmt.Sprintf("CMD: %s, Body: %s", commandline, body))

	if err != nil {
		response = err.Error()
		return
	}
	if result != nil {
		response = string(*result)
	}
	response = "OK"
	// if currentLen < dataLen {
	// 	buffer := make([]byte, dataLen)
	// 	for {
	// 		cnt, err := conn.Read(buffer)
	// 		fmt.Println(buffer)
	// 		data = append(data, buffer[:cnt]...)
	// 		currentLen = currentLen + cnt
	// 		if err != nil {
	// 			break
	// 		}
	// 		if currentLen >= dataLen {
	// 			break
	// 		}
	// 	}
	// }
}
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
	StartServing()
}