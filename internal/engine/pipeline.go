package engine

import (
	"github.com/dablelv/go-huge-util/conv"
	"strings"
)

func (c *Cake) openIndexPipeline(key string) chan IndexAndKey {
	indexChan := make(chan IndexAndKey, 1000)
	stream, err := c.dataDisk.ReadStream(key, false)
	if err != nil {
		panic(err)
	}

	meta := strings.Split(key, "_")
	shardId := conv.ToAny[int64](meta[1])
	createdAt := conv.ToAny[int64](meta[2])

	go func() {
		defer stream.Close()
		for {
			index := Index{}
			err := index.Read(stream)
			if err != nil {
				break
			}

			dataKey := key[:len(key)-5] + "data"

			indexChan <- IndexAndKey{
				Index:     index,
				DataKey:   dataKey,
				CreatedAt: createdAt,
				ShardId:   shardId,
			}
		}
		close(indexChan)
	}()
	return indexChan
}

func cmpIndexAndKey(a, b IndexAndKey) bool {
	if a.Did != b.Did {
		return a.Did < b.Did
	}
	if a.Timestamp != b.Timestamp {
		return a.Timestamp < b.Timestamp
	}
	return a.CreatedAt < b.CreatedAt
}

func merge(in1, in2 chan IndexAndKey) chan IndexAndKey {
	out := make(chan IndexAndKey, 1000)

	go func() {

		v1, ok1 := <-in1
		v2, ok2 := <-in2

		for ok1 || ok2 {
			if !ok2 || (ok1 && cmpIndexAndKey(v1, v2)) {
				out <- v1
				v1, ok1 = <-in1
			} else {
				out <- v2
				v2, ok2 = <-in2
			}
		}

		close(out)
	}()

	return out
}

func MergeN(inputs ...chan IndexAndKey) chan IndexAndKey {
	if len(inputs) == 1 {
		return inputs[0]
	}

	middle := len(inputs) / 2
	return merge(MergeN(inputs[:middle]...), MergeN(inputs[middle:]...))
}
