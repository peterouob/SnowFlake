package snowflake_test

import (
	"fmt"
	"snowflake/snowflake"
	"testing"
)

func TestNextId(t *testing.T) {
	idworker := snowflake.NewIdWorker(1, 1, 1, 5, 5, 12)
	for i := 0; i < 5; i++ {
		i := idworker.NextId()
		mi := i >> idworker.GetWorkerIdShift() &^ (1 << idworker.GetWorkerIdBits())
		fmt.Println(mi)
	}
}
