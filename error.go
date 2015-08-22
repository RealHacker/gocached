package main
import "errors"


var (
	CommandError = errors.New("GENERAL")
	ClientError = errors.New("CLIENT")
	ServerError = errors.New("SERVER")
)