package mysql

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestFormatQueryRewritesVectorColumnType verifies vector column definitions are rewritten into a
// downstream-compatible type. This matters because leaving unsupported vector types in DDL would
// cause DDL execution failures and stall replication.
// The key scenario is a CREATE TABLE statement containing a VECTOR column that must be converted.
func TestFormatQueryRewritesVectorColumnType(t *testing.T) {
	in := "CREATE TABLE test.t(id int primary key, data VECTOR(5) COMMENT 'vec');"
	out := formatQuery(in)
	require.NotEmpty(t, out)

	upper := strings.ToUpper(out)
	require.True(t, strings.Contains(upper, "LONGTEXT") || strings.Contains(upper, "LONGBLOB"),
		"expected rewritten query to use a compatible long value type")
	require.NotContains(t, upper, "VECTOR")
	require.NotContains(t, upper, "COMMENT")
}
