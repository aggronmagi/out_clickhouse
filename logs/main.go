package main

import (
	"fmt"
	"math/rand"
	"time"
)

func main() {
	for {
		nv := rand.Int31n(10)
		if nv < 0 {
			nv = -nv
		}
		fmt.Printf("%s evlog %d %s %s\n", time.Now().Format(time.RFC3339Nano), rand.Int63(), "test", "{\"key\":\"value\"}")
		time.Sleep(time.Duration(nv) * time.Millisecond * 300)
	}
}
