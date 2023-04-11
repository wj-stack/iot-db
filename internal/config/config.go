package config

import (
	"fmt"
	"github.com/mitchellh/mapstructure"
	"github.com/spf13/viper"
)

type Config struct {
	Workspace string `json:"workspace"`
	SaveFile  int    `json:"savefile"`
}

var Default Config

func init() {
	viper.SetConfigName("config.yaml")
	viper.SetConfigType("yaml")

	viper.AddConfigPath("./configs/")
	err := viper.ReadInConfig()
	if err != nil {
		panic(fmt.Errorf("Fatal error config file: %s \n", err))
	}

	err = viper.Unmarshal(&Default, func(config *mapstructure.DecoderConfig) {
		config.TagName = "json"
	})
	if err != nil {
		panic(err)
	}

}
