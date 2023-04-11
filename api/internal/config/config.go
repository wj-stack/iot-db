package config

import "github.com/zeromicro/go-zero/rest"

type Config struct {
	rest.RestConf
	Workspace string `json:"workspace"`
	SaveFile  int    `json:"savefile"`
}
