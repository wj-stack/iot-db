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

	// query 2022-11-27 08:11:15.659376888 +0000 UTC m=-12700799.939801161 2022-12-04 08:11:15.659376888 +0000 UTC m=-12095999.939801161
	for j := time.Now(); j.After(time.Now().AddDate(-1, 0, 0)); j = j.Add(-time.Hour * 24 * 7) {
		for i := 0; i < 100; i++ {
			t := time.Now()
			//a, _ := time.Parse(time.RFC3339Nano, "2022-11-27T08:11:15.659Z")
			//b, _ := time.Parse(time.RFC3339Nano, "2022-12-24T08:11:15.659Z")
			query, err := e.Query(26754, j.Add(-time.Hour*24*7).UnixNano(), j.UnixNano())
			if err != nil {
				return
			}
			cnt := 0
			for _ = range query {
				cnt++
			}
			logrus.Infoln("cnt:", cnt, time.Now().UnixMilli()-t.UnixMilli())
		}
	}

	//t := time.Now()
	//var wg sync.WaitGroup
	//for j := 0; j < 1; j++ {
	//	wg.Add(1)
	//	go func() {
	//		defer wg.Done()
	//		for j := time.Now(); j.After(time.Now().AddDate(-1, 0, 0)); j = j.Add(-time.Hour * 24 * 7) {
	//			t := time.Now()
	//			dataChan, err := e.Query(26754, j.Add(-time.Hour*24*7).UnixNano(), j.UnixNano())
	//			if err != nil {
	//				panic(err)
	//			}
	//			cnt := 0
	//			for _ = range dataChan {
	//				cnt++
	//				//logrus.Infoln(data)
	//			}
	//			logrus.Infoln("query", j.Add(-time.Hour*24*7), j)
	//			if cnt > 0 {
	//				logrus.Infoln(time.Now().UnixMilli()-t.UnixMilli(), "cnt:", cnt)
	//			}
	//		}
	//	}()
	//}
	//wg.Wait()
	//logrus.Infoln(time.Now().UnixMilli() - t.UnixMilli())

}
