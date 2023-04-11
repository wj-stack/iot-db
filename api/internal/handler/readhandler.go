package handler

import (
	"net/http"

	"github.com/zeromicro/go-zero/rest/httpx"
	"iot-db/api/internal/logic"
	"iot-db/api/internal/svc"
)

func ReadHandler(svcCtx *svc.ServiceContext) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		l := logic.NewReadLogic(r.Context(), svcCtx)
		resp, err := l.Read()
		if err != nil {
			httpx.ErrorCtx(r.Context(), w, err)
		} else {
			httpx.OkJsonCtx(r.Context(), w, resp)
		}
	}
}
