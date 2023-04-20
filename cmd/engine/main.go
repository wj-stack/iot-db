package main

import (
	"github.com/sirupsen/logrus"
	"iot-db/internal/engine"
	"net/http"
	"net/http/pprof"
	"time"
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

	for i := time.Now(); i.After(time.Now().AddDate(-1, 0, 0)); i = i.Add(-time.Hour * 24 * 7) {
		t := time.Now()
		dataChan, err := e.Query(26754, i.Add(-time.Hour*24*7).UnixNano(), i.UnixNano())
		if err != nil {
			panic(err)
		}
		cnt := 0
		for _ = range dataChan {
			cnt++
			//logrus.Infoln(data)
		}
		if cnt > 0 {
			logrus.Infoln("query", i.Add(-time.Hour*24*7), i)
			logrus.Infoln(time.Now().UnixMilli()-t.UnixMilli(), "cnt:", cnt)
		}
	}

}
