//  Copyright 2020-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included in
//  the file licenses/Couchbase-BSL.txt.  As of the Change Date specified in that
//  file, in accordance with the Business Source License, use of this software will
//  be governed by the Apache License, Version 2.0, included in the file
//  licenses/APL.txt.

package execution

import (
	"encoding/json"

	//	"github.com/couchbase/query/datastore"
	"github.com/couchbase/query/errors"
	"github.com/couchbase/query/plan"
	"github.com/couchbase/query/value"
)

type CreateCollection struct {
	base
	plan *plan.CreateCollection
}

func NewCreateCollection(plan *plan.CreateCollection, context *Context) *CreateCollection {
	rv := &CreateCollection{
		plan: plan,
	}

	newRedirectBase(&rv.base)
	rv.output = rv
	return rv
}

func (this *CreateCollection) Accept(visitor Visitor) (interface{}, error) {
	return visitor.VisitCreateCollection(this)
}

func (this *CreateCollection) Copy() Operator {
	rv := &CreateCollection{plan: this.plan}
	this.base.copy(&rv.base)
	return rv
}

func (this *CreateCollection) PlanOp() plan.Operator {
	return this.plan
}

func (this *CreateCollection) RunOnce(context *Context, parent value.Value) {
	this.once.Do(func() {
		defer context.Recover(&this.base) // Recover from any panic
		active := this.active()
		defer this.close(context)
		this.switchPhase(_EXECTIME)
		defer this.switchPhase(_NOTIME)
		defer this.notify() // Notify that I have stopped

		if !active || context.Readonly() {
			return
		}

		// Actually create collection
		this.switchPhase(_SERVTIME)
		err := this.plan.Scope().CreateCollection(this.plan.Node().Name())
		if err != nil {
			if !errors.IsCollectionExistsError(err) || this.plan.Node().FailIfExists() {
				context.Error(err)
			}
		}
	})
}

func (this *CreateCollection) MarshalJSON() ([]byte, error) {
	r := this.plan.MarshalBase(func(r map[string]interface{}) {
		this.marshalTimes(r)
	})
	return json.Marshal(r)
}
