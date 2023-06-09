// Code generated by goctl. DO NOT EDIT.
package handler

import (
	"net/http"

	"iot-db/api/internal/svc"

	"github.com/zeromicro/go-zero/rest"
)

func RegisterHandlers(server *rest.Server, serverCtx *svc.ServiceContext) {
	server.AddRoutes(
		[]rest.Route{
			{
				Method:  http.MethodGet,
				Path:    "/write/:did/timestamp",
				Handler: WriteHandler(serverCtx),
			},
			{
				Method:  http.MethodGet,
				Path:    "/read/:did/:start/:end",
				Handler: ReadHandler(serverCtx),
			},
		},
	)
}
