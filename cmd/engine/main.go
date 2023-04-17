package main

import (
	"github.com/sirupsen/logrus"
	"iot-db/internal/engine"
	"net/http"
	"net/http/pprof"
)

const (
	pprofAddr string = ":7890"
)

func StartHTTPDebuger() {
	pprofHandler := http.NewServeMux()
	pprofHandler.Handle("/debug/pprof/", http.HandlerFunc(pprof.Index))
	server := &http.Server{Addr: pprofAddr, Handler: pprofHandler}
	go server.ListenAndServe()
}
func main() {
	StartHTTPDebuger()
	e := engine.NewDefaultEngine()
	//t := time.Now()
	//e.Query(26754, 0, time.Now().UnixNano())
	//logrus.Infoln(time.Now().UnixMilli() - t.UnixMilli())
	_, err := e.Compact()
	if err != nil {
		logrus.Fatalln(err)
	}
}
