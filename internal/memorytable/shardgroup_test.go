package memorytable

import (
	"strings"
	"testing"
	"time"
)

func TestShardGroup_Insert(t *testing.T) {
	shardGroup := NewShardGroup()
	body := []byte(strings.Repeat("A", 100))
	for i := 0; i < 1000; i++ {
		for j := 0; j < 1000; j++ {
			shardGroup.Insert(int64(i), int64(j), body)
		}
	}
	time.Sleep(time.Second * 3)
}
