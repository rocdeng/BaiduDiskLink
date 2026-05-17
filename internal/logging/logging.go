package logging

import "log"

func New() *log.Logger {
	return log.Default()
}
