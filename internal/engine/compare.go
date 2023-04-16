package engine

import "iot-db/internal/pb"

// 	// < 0 when a < b
//	// == 0 when a == b
//	// > 0 when a > b
//	Compare(a T, b T) int

type DataCompare struct {
}

func (c *DataCompare) Compare(a *pb.Data, b *pb.Data) int {

	if a.GetDid() < b.GetDid() {
		return -1
	} else if a.GetDid() > b.GetDid() {
		return 1
	}

	if a.GetTimestamp() < b.GetTimestamp() {
		return -1
	} else if a.GetTimestamp() > b.GetTimestamp() {
		return 1
	}

	//if a.GetCreatedAt() < b.GetCreatedAt() {
	//	return -1
	//} else if a.GetCreatedAt() > b.GetCreatedAt() {
	//	return 1
	//}

	return 0
}
