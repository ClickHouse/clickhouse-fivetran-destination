package types

import (
	"fmt"

	"fivetran.com/fivetran_sdk/destination/common/constants"
)

func ToClusterMacros(macroMap map[string]string) (*ClusterMacros, error) {
	cluster, ok := macroMap[constants.MacroCluster]
	if !ok {
		return nil, fmt.Errorf("macro %s is missing", constants.MacroCluster)
	}
	replica, ok := macroMap[constants.MacroReplica]
	if !ok {
		return nil, fmt.Errorf("macro %s is missing", constants.MacroReplica)
	}
	shard, ok := macroMap[constants.MacroShard]
	if !ok {
		return nil, fmt.Errorf("macro %s is missing", constants.MacroShard)
	}
	return &ClusterMacros{
		Cluster: cluster,
		Replica: replica,
		Shard:   shard,
	}, nil
}
