package compactor

import "container/heap"

type PQItem struct {
	value    FileFd // 存储的值
	priority int    // 优先级
	index    int    // 在堆中的索引
}

type PriorityQueue []*PQItem

// Len 实现 heap.Interface 接口
func (pq PriorityQueue) Len() int { return len(pq) }
func (pq PriorityQueue) Less(i, j int) bool {
	return pq[i].priority < pq[j].priority
}
func (pq PriorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}

// Push 实现 heap.Interface 接口的 Push 方法
func (pq *PriorityQueue) Push(x interface{}) {
	n := len(*pq)
	item := x.(*PQItem)
	item.index = n
	*pq = append(*pq, item)
}

// Pop 实现 heap.Interface 接口的 Pop 方法
func (pq *PriorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	item.index = -1 // 为了安全，避免使用后的 item 继续被访问
	*pq = old[0 : n-1]
	return item
}

// 更新 PQItem 的优先级和值
func (pq *PriorityQueue) update(item *PQItem, value FileFd, priority int) {
	item.value = value
	item.priority = priority
	heap.Fix(pq, item.index)
}
