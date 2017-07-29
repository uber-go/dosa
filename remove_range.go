package dosa

// RemoveRangeOp is used to specify contraints on RemoveRange calls
type RemoveRangeOp struct {
	conditioner
}

// NewRemoveRangeOp returns a new RangeOp instance
func NewRemoveRangeOp(object DomainObject) *RemoveRangeOp {
	rop := &RemoveRangeOp{
		conditioner: conditioner{
			object:     object,
			conditions: map[string][]*Condition{},
		},
	}
	return rop
}

// Eq is used to express an equality constraint for a remove range operation
func (r *RemoveRangeOp) Eq(fieldName string, value interface{}) *RemoveRangeOp {
	r.appendOp(Eq, fieldName, value)
	return r
}

// Gt is used to express an "greater than" constraint for a remove range operation
func (r *RemoveRangeOp) Gt(fieldName string, value interface{}) *RemoveRangeOp {
	r.appendOp(Gt, fieldName, value)
	return r
}

// GtOrEq is used to express an "greater than or equal" constraint for a
// remove range operation
func (r *RemoveRangeOp) GtOrEq(fieldName string, value interface{}) *RemoveRangeOp {
	r.appendOp(GtOrEq, fieldName, value)
	return r
}

// Lt is used to express a "less than" constraint for a remove range operation
func (r *RemoveRangeOp) Lt(fieldName string, value interface{}) *RemoveRangeOp {
	r.appendOp(Lt, fieldName, value)
	return r
}

// LtOrEq is used to express a "less than or equal" constraint for a
// remove range operation
func (r *RemoveRangeOp) LtOrEq(fieldName string, value interface{}) *RemoveRangeOp {
	r.appendOp(LtOrEq, fieldName, value)
	return r
}
