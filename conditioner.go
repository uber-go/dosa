package dosa

import "github.com/pkg/errors"

type conditioner struct {
	object     DomainObject
	conditions map[string][]*Condition
}

func (c *conditioner) appendOp(op Operator, fieldName string, value interface{}) {
	c.conditions[fieldName] = append(c.conditions[fieldName], &Condition{Op: op, Value: value})
}

// convertConditions converts a list of client field names to server side field names
func convertConditions(conditions map[string][]*Condition, t *Table) (map[string][]*Condition, error) {
	serverConditions := map[string][]*Condition{}
	for colName, conds := range conditions {
		if scolName, ok := t.FieldToCol[colName]; ok {
			serverConditions[scolName] = conds
			// we need to be sure each of the types are correct for marshaling
			cd := t.FindColumnDefinition(scolName)
			for _, cond := range conds {
				if err := ensureTypeMatch(cd.Type, cond.Value); err != nil {
					return nil, errors.Wrapf(err, "column %s", colName)
				}
			}
		} else {
			return nil, errors.Errorf("Cannot find column %q in struct %q", colName, t.StructName)
		}
	}
	return serverConditions, nil
}
