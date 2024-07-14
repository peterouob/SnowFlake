package main

import (
	"fmt"
	"snowflake/snowflake"
)

func main() {
	idworker := snowflake.NewIdWorker(1, 1, 0, 5, 5, 12)
	i := idworker.NextId()
	fmt.Println("Generated ID:", i)

	mi := idworker.GetMachineId()
	fmt.Println("Worker ID:", mi)

	di := idworker.GetDatacenterId()
	fmt.Println("Datacenter ID:", di)

	ti := idworker.GetTimeStamp()
	fmt.Println("Timestamp:", ti)
}
