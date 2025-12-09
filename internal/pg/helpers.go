package pg

import "database/sql/driver"

// QuoteIdentifier safely quotes a PostgreSQL identifier.
func QuoteIdentifier(s string) string {
	return `"` + EscapeString(s) + `"`
}

// EscapeString escapes quotes (for identifiers only — not for values!).
func EscapeString(s string) string {
	return driver.ErrRemoveArgument(s)
}