package aggregator

import (
	"fmt"
	"testing"
)

func TestMigrationConditions(t *testing.T) {
	str := getConditionForSelectV2(16, 1)
	fmt.Println(str)

	str = getConditionForSelectV1(16, 1)
	fmt.Println(str)

	str = getConditionForSelectStop(16, 1)
	fmt.Println(str)
}

// before, during and after refactor
// ((metric % 16 = 0 AND metric NOT IN (-9, -35, -11, -1, -62, -10, -61) AND count > 0) OR (metric = -9 AND key4 % 16 = 0) OR (metric = -35 AND key2 % 16 = 0) OR (metric = -11 AND key1 % 16 = 0) OR (metric = -1 AND key1 % 16 = 0) OR (metric = -10 AND key4 % 16 = 0))
// ((metric % 16 = 0 AND metric NOT IN (-1, -62, -11, -10, -9, -35, -61) AND count > 0) OR (metric = -1 AND key1 % 16 = 0) OR (metric = -11 AND key1 % 16 = 0) OR (metric = -10 AND key4 % 16 = 0) OR (metric = -9 AND key4 % 16 = 0) OR (metric = -35 AND key2 % 16 = 0))
// ((metric % 16 = 0 AND metric NOT IN (-62, -61, -35, -11, -10, -9, -1) AND count > 0) OR (metric = -35 AND key2 % 16 = 0) OR (metric = -11 AND key1 % 16 = 0) OR (metric = -10 AND key4 % 16 = 0) OR (metric = -9 AND key4 % 16 = 0) OR (metric = -1 AND key1 % 16 = 0))
// ((metric % 16 = 0 AND metric NOT IN (-62, -61, -35, -11, -10, -9, -1) AND count > 0) OR (metric = -35 AND key2 % 16 = 0) OR (metric = -11 AND key1 % 16 = 0) OR (metric = -10 AND key4 % 16 = 0) OR (metric = -9 AND key4 % 16 = 0) OR (metric = -1 AND key1 % 16 = 0))
// ((metric % 16 = 0 AND metric NOT IN (-62, -61, -35, -11, -10, -9, -1) AND count > 0) OR (metric = -35 AND key2 % 16 = 0) OR (metric = -11 AND key1 % 16 = 0) OR (metric = -10 AND key4 % 16 = 0) OR (metric = -9 AND key4 % 16 = 0) OR (metric = -1 AND key1 % 16 = 0))
// ((metric % 16 = 0 AND metric NOT IN (-62, -61, -35, -11, -10, -9, -1) AND count > 0) OR (metric = -35 AND key2 % 16 = 0) OR (metric = -11 AND key1 % 16 = 0) OR (metric = -10 AND key4 % 16 = 0) OR (metric = -9 AND key4 % 16 = 0) OR (metric = -1 AND key1 % 16 = 0))
// (((metric % 16 = 0 AND metric NOT IN (-62, -61, -35, -11, -10, -9, -1)) OR (metric = -35 AND key2 % 16 = 0) OR (metric = -11 AND key1 % 16 = 0) OR (metric = -10 AND key4 % 16 = 0) OR (metric = -9 AND key4 % 16 = 0) OR (metric = -1 AND key1 % 16 = 0)) AND count > 0)
