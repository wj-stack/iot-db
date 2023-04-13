package config

import (
	"fmt"
	"github.com/mitchellh/mapstructure"
	"github.com/spf13/viper"
)

type Config struct {
	Core struct {
		Path struct {
			Data    string `json:"data"`
			Wal     string `json:"wal"`
			Rubbish string `json:"rubbish"`
		} `json:"path"`
		ShardGroupSize []int `json:"shard_group_size"`
		FragmentSize   int   `json:"fragment_size"`
	} `json:"core"`
	Compactor struct {
		LatestFileSize int   `json:"latest_file_size"` // Keep the latest n files and do not participate in the merger
		FragmentSize   []int `json:"fragment_size"`    // The maximum size of each fragment
	} `json:"compactor"`
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
