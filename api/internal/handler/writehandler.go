package handler

import (
	"net/http"

	"github.com/zeromicro/go-zero/rest/httpx"
	"iot-db/api/internal/logic"
	"iot-db/api/internal/svc"
	"iot-db/api/internal/types"
)

func WriteHandler(svcCtx *svc.ServiceContext) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req types.WriteRequest
		if err := httpx.Parse(r, &req); err != nil {
			httpx.ErrorCtx(r.Context(), w, err)
			return
		}

		l := logic.NewWriteLogic(r.Context(), svcCtx)
		resp, err := l.Write(&req)
		if err != nil {
			httpx.ErrorCtx(r.Context(), w, err)
		} else {
			httpx.OkJsonCtx(r.Context(), w, resp)
		}
	}
}
