package skiplist

import (
	"github.com/sirupsen/logrus"
	"testing"
)

func TestMap_Insert(t *testing.T) {
	list := NewSkipListMap[int64, int64](OrderedComparator[int64]{})
	list.Insert(1, 1)
	list.Insert(1, 2)
	list.Insert(2, 1)
	list.Insert(3, 1)
	iterator, err := list.Iterator()
	if err != nil {
		return
	}
	for {
		next, i, err := iterator.Next()
		if err != nil {
			return
		}
		logrus.Infoln(next, i)
	}
}
