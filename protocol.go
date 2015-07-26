package main

import (
	"strings"
	"strconv"
	"errors"
)


func HandleClientRequest(commandline string, body *[]byte) (val *[]byte, err error) {
	// <command name> <key> <flags> <exptime> <bytes> [noreply]\r\n
	components := strings.Split(commandline, " ")
	if len(components) < 5 {
		err = errors.New("Not enough components in command line")
		return
	}
	
	command := strings.Trim(components[0], " \t")
	key := strings.Trim(components[1], " \t")
	// flags := strings.Trim(components[2], " \t")
	exptime := strings.Trim(components[3], " \t")
	bytes := strings.Trim(components[4], " \t")

	bytecnt, err := strconv.Atoi(bytes)
	if err != nil {
		return
	}
	ttlint, err := strconv.Atoi(exptime)
	if err != nil {
		return
	}
	ttl := int64(ttlint)
	if ttl < 0 {
		err = errors.New("ttl cannot be negative")
		return
	}

	if len(*body) < bytecnt {
		err = errors.New("Body byte count not correct")
		return
	}

	switch command {
	case "GET":
		val = Get(key)
		if val == nil {
			err = errors.New("missing value for key")
		}
	case "SET":
		val = nil
		err = Set(key, body, ttl)
	case "DELETE":
		val = nil
		err = Delete(key)
	default:
		val = nil
		err = errors.New("invalid command")
	}
	return
}