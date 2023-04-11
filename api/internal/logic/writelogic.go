package logic

import (
	"context"

	"iot-db/api/internal/svc"
	"iot-db/api/internal/types"

	"github.com/zeromicro/go-zero/core/logx"
)

type WriteLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewWriteLogic(ctx context.Context, svcCtx *svc.ServiceContext) *WriteLogic {
	return &WriteLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *WriteLogic) Write(req *types.WriteRequest) (resp *types.WriteResponse, err error) {
	// todo: add your logic here and delete this line

	return
}
