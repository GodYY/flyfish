package node_sql

import "teacher/node/common/config"

var (
	conf    *config.Config
	nodeIdx int
)

func initConfig(c *config.Config, idx int) {
	conf = c
	nodeIdx = idx
}

func getNodeConfig() *config.Sql {
	return conf.Sql[nodeIdx]
}
