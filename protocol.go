package main

import (
	"strings"
	"strconv"
	"errors"
	"net"
	"bytes"
	"fmt"
)

// currently only 4 primary commands are supported
const (
	SET_COMMAND int64 = iota
	GET_COMMAND
	DELETE_COMMAND
	TOUCH_COMMAND
)

type Command struct {
	cmdType int64
	cmd string
	key string
	bodyLen int64
	flags int64
	ttl int64
}

func writeLog(logstr string){
	fmt.Println(logstr)
}

func closeConn(conn net.Conn){
	if err := conn.Close(); err != nil {
		fmt.Println("Fail to close connection")
	}
}

// A safe method to write response
func writeResponse(conn net.Conn, message []byte) error{
	sent := 0
	for {
		cnt, err := conn.Write(message[sent:])
		if err != nil {
			writeLog("Fail to send response due to error:"+err.Error())
			return err
		}
		sent += cnt
		if sent >= len(message) || cnt ==0{
			break
		}
	}
	if sent < len(message) {
		return errors.New("Fail to send complete response")
	}
	return nil
}

func returnError(conn net.Conn, err error, message string) {
	var msg string
	switch err {
	case CommandError:
		msg = "ERROR\r\n"
	case ClientError:
		msg = fmt.Sprintf("CLIENT_ERROR %s\r\n", message)
	case ServerError:
		msg = fmt.Sprintf("SERVER_ERROR %s\r\n", message)
	default:
		msg = "ERROR\r\n"
	}
	err = writeResponse(conn, []byte(msg))
	if err != nil{
		writeLog("Fail to send Error response:"+err.Error())
	}
}

func returnGetResponse(conn net.Conn, key string, value []byte, flags int64){
	firstLineStr := fmt.Sprintf("VALUE %s %d %d \r\n", key, flags, len(value))
	firstLine := []byte(firstLineStr)
	lastLineStr := "\r\nEND\r\n"
	lastLine := []byte(lastLineStr)
	var parts [][]byte
	
	parts = append(parts, firstLine)
	parts = append(parts, value)
	parts = append(parts, lastLine)

	var blank []byte
	response := bytes.Join(parts, blank)

	err := writeResponse(conn, response)
	if err != nil {
		writeLog("Fail to send GET response")
	}
}

func returnSetSuccess(conn net.Conn){
	response := []byte("STORED\r\n")
	err := writeResponse(conn, response)
	if err != nil {
		writeLog("Fail to send SET success response")
	}
}

func returnDeleteSuccess(conn net.Conn){
	response := []byte("DELETED\r\n")
	err := writeResponse(conn, response)
	if err != nil {
		writeLog("Fail to send DELETE success response")
	}
}

func returnTouchSuccess(conn net.Conn){
	response := []byte("TOUCHED\r\n")
	err := writeResponse(conn, response)
	if err != nil {
		writeLog("Fail to send TOUCH success response")
	}
}

func parseCommand(commandline []byte) (*Command, error) {
	// <command name> <key> <flags> <exptime> <bytes> [noreply]\r\n
	components := strings.Split(string(commandline), " ")
	
	cmd := strings.Trim(components[0], " \t")
	command := Command{
		cmd: cmd,
	}
	switch cmd {
	case "delete":
		command.cmdType = DELETE_COMMAND
		if len(components)<2 {
			return nil, errors.New("DELETE COMMAND lacks key")
		}
		command.key = strings.Trim(components[1], " \t")
	case "get":
		command.cmdType = GET_COMMAND
		if len(components)<2 {
			return nil, errors.New("GET COMMAND lacks key")
		}
		command.key = strings.Trim(components[1], " \t")
	case "touch":
		command.cmdType = TOUCH_COMMAND
		if len(components)<3 {
			return nil, errors.New("GET COMMAND lacks key or exptime")
		}
		command.key = strings.Trim(components[1], " \t")

		exptime := strings.Trim(components[2], " \t")
		ttl, err := strconv.Atoi(exptime)
		if err != nil {
			return nil, errors.New("TOUCH COMMAND ttl should be int")
		}
		command.ttl = int64(ttl)
	case "set":
		command.cmdType = SET_COMMAND
		if len(components)<5 {
			return nil, errors.New("SET COMMAND missing components")
		}
		command.key = strings.Trim(components[1], " \t")
		
		flagstr := strings.Trim(components[2], " \t")
		flags, err := strconv.Atoi(flagstr)
		if err != nil {
			return nil, errors.New("SET COMMAND flags should be int")
		}
		command.flags = int64(flags)

		exptime := strings.Trim(components[3], " \t")
		ttl, err := strconv.Atoi(exptime)
		if err != nil {
			return nil, errors.New("SET COMMAND ttl should be int")
		}
		command.ttl = int64(ttl)

		bodylenstr := strings.Trim(components[4], " \t")
		bodylen, err := strconv.Atoi(bodylenstr)
		if err != nil {
			return nil, errors.New("SET COMMAND bodylen should be int")
		}
		command.bodyLen = int64(bodylen)
	default:
		return nil, errors.New("Command not recognized")
	}
	return &command, nil
}

func ConnectionHandler(conn net.Conn){
	defer closeConn(conn)
	for{
		// First Read the command line, the command line could never be more than 512 bytes
		// So we make a 512 bytes buffer, and save the bytes after '\r\n' as starting part of body (for save commands) 
		commandBuffer := make([]byte, 512)

		sep := []byte("\r\n")

		cnt, err := conn.Read(commandBuffer) // What if the first read doesn't load entire command line?
		writeLog("RECV:"+string(commandBuffer))
		if err != nil {
			returnError(conn, ClientError, "Command Read Error")
			break
		}

		// TODO: when receiving junk, we can still read everything until timeout,
		// discard them, and wait for the next valid command. 
		// That is a more robust implementation. 
		// For now, we just close connection on junk data.
		commandLen := bytes.Index(commandBuffer, sep)		
		if commandLen < 0 {
			returnError(conn, ClientError, "Command Line Format Error, \\r\\n not found")
			break
		}
		command, err := parseCommand(commandBuffer[:commandLen])
		if err != nil {
			returnError(conn, CommandError, "Invalid command: " + err.Error())
			break
		}

		// read the rest of body if necessary
		var body []byte
		if command.cmdType == SET_COMMAND {
			if command.bodyLen <= int64(cnt - (commandLen+4)) {
				body = commandBuffer[commandLen+2:commandLen+2+int(command.bodyLen)]			
			}else{
				bodyBuffer := commandBuffer[commandLen+2:cnt]
				var fragments [][]byte
				// Put the first fragment in
				fragments = append(fragments, bodyBuffer) 

				remaining := int(command.bodyLen) - cnt + (commandLen+4)
				for remaining > 0 {
					buffer := make([]byte, remaining)
					cnt, err := conn.Read(buffer)
					if err != nil {
						break
					}
					fragments = append(fragments, buffer[:cnt])
					remaining -= cnt
				}
				if remaining > 0 {
					// Fail to read a complete message, close connection
					returnError(conn, ClientError, "Incomplete command message")
					break
				}
				var blank []byte
				body = bytes.Join(fragments, blank)
				body = body[:len(body)-2] //drop the last "\r\n"		
			}
		} 

		// handle the command and body
		err = HandleClientRequest(conn, *command, &body)
		if err != nil {
			returnError(conn, ServerError, "Fail to process command")
		}
	}

	
	// firstline, err := reader.ReadBytes(splitter[0])
	// if err != nil {
	// 	fmt.Println("Not enough chars in command")
	// 	return
	// }

	// index := bytes.Index(firstline, splitter)

	// commandline := string(firstline[:index])
	// body, err := reader.ReadBytes(splitter[0])
	// body = body[1:len(body)-1] 	//escape \n

	// protocol parsing
	// result, err := 
	// fmt.Println(fmt.Sprintf("CMD: %s, Body: %s", commandline, body))

	// if err != nil {
	// 	response = err.Error()
	// 	return
	// }
	// if result != nil {
	// 	response = string(*result)
	// }
	// response = "OK"
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

func HandleClientRequest(conn net.Conn, command Command, body *[]byte) (err error) {
	writeLog(command.cmd)
	switch command.cmdType {
	case GET_COMMAND:
		val, flags := Get(command.key)
		if val == nil {
			err = errors.New("missing value for key")
		}else{
			returnGetResponse(conn, command.key, *val, flags)
		}
	case SET_COMMAND:
		err = Set(command.key, body, command.ttl, command.flags)
		if err == nil {
			returnSetSuccess(conn)
		}
	case DELETE_COMMAND:
		err = Delete(command.key)
		if err == nil {
			returnDeleteSuccess(conn)
		}
	case TOUCH_COMMAND:
		err = Touch(command.key, command.ttl)
		if err == nil {
			returnTouchSuccess(conn)
		}
	default:
		err = errors.New("invalid command")
	}
	return
}