package skiplist

import (
	"testing"
)

func TestMap_Insert(t *testing.T) {
	list := NewSkipListMap[int64, int64](OrderedComparator[int64]{})

	for i := 0; i < 500; i++ {
		list.Insert(int64(i), int64(i))
	}

	iterator, err := list.Iterator()

	if err != nil {
		return
	}
	for {
		_, _, err := iterator.Next()
		if err != nil {
			return
		}
		//logrus.Infoln(next, i)
	}

}
