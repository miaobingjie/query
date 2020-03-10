//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package plan

import (
	"encoding/json"
	"fmt"

	"github.com/couchbase/query/algebra"
	"github.com/couchbase/query/expression"
	"github.com/couchbase/query/expression/parser"
)

// ValueScan is used for VALUES clauses, e.g. in INSERTs.
type ValueScan struct {
	readonly
	values      algebra.Pairs
	cost        float64
	cardinality float64
}

func NewValueScan(values algebra.Pairs, cost, cardinality float64) *ValueScan {
	return &ValueScan{
		values:      values,
		cost:        cost,
		cardinality: cardinality,
	}
}

func (this *ValueScan) Accept(visitor Visitor) (interface{}, error) {
	return visitor.VisitValueScan(this)
}

func (this *ValueScan) New() Operator {
	return &ValueScan{}
}

func (this *ValueScan) Values() algebra.Pairs {
	return this.values
}

func (this *ValueScan) Cost() float64 {
	return this.cost
}

func (this *ValueScan) Cardinality() float64 {
	return this.cardinality
}

func (this *ValueScan) MarshalJSON() ([]byte, error) {
	return json.Marshal(this.MarshalBase(nil))
}

func (this *ValueScan) MarshalBase(f func(map[string]interface{})) map[string]interface{} {
	r := map[string]interface{}{"#operator": "ValueScan"}
	r["values"] = this.values.Expression().String()
	if this.cost > 0.0 {
		r["cost"] = this.cost
	}
	if this.cardinality > 0.0 {
		r["cardinality"] = this.cardinality
	}
	if f != nil {
		f(r)
	}
	return r
}

func (this *ValueScan) UnmarshalJSON(body []byte) error {
	var _unmarshalled struct {
		_           string  `json:"#operator"`
		Values      string  `json:"values"`
		Cost        float64 `json:"cost"`
		Cardinality float64 `json:"cardinality"`
	}

	err := json.Unmarshal(body, &_unmarshalled)
	if err != nil {
		return err
	}

	if _unmarshalled.Values == "" {
		return nil
	}

	expr, err := parser.Parse(_unmarshalled.Values)
	if err != nil {
		return err
	}

	array, ok := expr.(*expression.ArrayConstruct)
	if !ok {
		return fmt.Errorf("Invalid VALUES expression %s", _unmarshalled.Values)
	}

	this.values, err = algebra.NewValuesPairs(array)
	if err != nil {
		return err
	}

	this.cost = getCost(_unmarshalled.Cost)
	this.cardinality = getCardinality(_unmarshalled.Cardinality)

	return nil
}
