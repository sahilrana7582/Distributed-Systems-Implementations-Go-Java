package config

import (
	"io/ioutil"

	"gopkg.in/yaml.v3"
)

type Peer struct {
	ID   int    `yaml:"id"`
	Addr string `yaml:"addr"`
}

type RaftConfig struct {
	Bind        string `yaml:"bind"`
	Peers       []Peer `yaml:"peers"`
	HeartbeatMs int    `yaml:"heartbeatMs"`
	DataDir     string `yaml:"dataDir"`
}

type Config struct {
	Raft RaftConfig `yaml:"raft"`
}

func Load(path string) (*Config, error) {
	b, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var c Config
	if err := yaml.Unmarshal(b, &c); err != nil {
		return nil, err
	}
	return &c, nil
}
