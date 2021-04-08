package dbfs

// TablesInfo is a structured representation of the contents in the tables.info file.
type TablesInfo struct {
	Tables map[string]string `yaml:"tables"`
	Count  int               `yaml:"count"`
}
