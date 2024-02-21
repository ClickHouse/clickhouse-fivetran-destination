package types

// MakeTableDescription ensures that a valid types.TableDescription is created from a list of types.ColumnDefinition.
func MakeTableDescription(columnDefinitions []*ColumnDefinition) *TableDescription {
	if len(columnDefinitions) == 0 {
		return &TableDescription{}
	}
	mapping := make(map[string]*ColumnDefinition, len(columnDefinitions))
	var primaryKeys []string
	for _, col := range columnDefinitions {
		mapping[col.Name] = col
		if col.IsPrimaryKey {
			primaryKeys = append(primaryKeys, col.Name)
		}
	}
	return &TableDescription{
		Mapping:     mapping,
		Columns:     columnDefinitions,
		PrimaryKeys: primaryKeys,
	}
}
