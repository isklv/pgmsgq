package pg

// QuoteIdentifier safely quotes a PostgreSQL identifier.
func QuoteIdentifier(s string) string {
	return `"` + s + `"`
}
