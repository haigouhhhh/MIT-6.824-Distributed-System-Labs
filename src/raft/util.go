package raft

import (
	"fmt"
	"time"
)

// Debugging

func (rf *Raft)DPrintf(format string, a ...interface{}) (n int, err error) {
	if rf.Debug > 0 {
		//log.Printf(format, a...)
		fmt.Println(time.Now().Format(time.StampMilli) + " " + fmt.Sprintf(format, a...))
	}
	return
}
