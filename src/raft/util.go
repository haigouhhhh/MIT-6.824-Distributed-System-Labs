package raft

import (
	"fmt"
	"time"
)

// Debugging
const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		//log.Printf(format, a...)
		fmt.Println(time.Now().Format(time.StampMilli) + " " + fmt.Sprintf(format, a...))
	}
	return
}
