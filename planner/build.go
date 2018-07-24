//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package planner

import (
	"strings"

	"github.com/couchbase/query/algebra"
	"github.com/couchbase/query/datastore"
	"github.com/couchbase/query/expression"
	"github.com/couchbase/query/plan"
	"github.com/couchbase/query/value"
)

func Build(stmt algebra.Statement, datastore, systemstore datastore.Datastore,
	namespace string, subquery bool, namedArgs map[string]value.Value,
	positionalArgs value.Values, indexApiVersion int, featureControls uint64) (plan.Operator, error) {
	builder := newBuilder(datastore, systemstore, namespace, subquery, namedArgs, positionalArgs,
		indexApiVersion, featureControls)
	o, err := stmt.Accept(builder)

	if err != nil {
		return nil, err
	}

	op := o.(plan.Operator)
	_, is_prepared := o.(*plan.Prepared)

	if !subquery && !is_prepared {
		privs, er := stmt.Privileges()
		if er != nil {
			return nil, er
		}

		// Always insert an Authorize operator, even if no privileges need to
		// be verified.
		//
		// We do this because the list of authenticated users is generated as
		// part of authentication, and this list may be needed in the query
		// (see the function CURRENT_USERS()).
		//
		// This should not impose a burden in production because every real
		// query is against secured tables anyway, and would therefore
		// have privileges that need verification, meaning the Authorize
		// operator would have been present in any case.
		op = plan.NewAuthorize(privs, op)

		return plan.NewSequence(op, plan.NewStream()), nil
	} else {
		return op, nil
	}
}

var _MAP_KEYSPACE_CAP = 4

const (
	BUILDER_WHERE_IS_TRUE  = 1 << iota // WHERE clause is TRUE
	BUILDER_WHERE_IS_FALSE             // WHERE clause is FALSE
)

type builder struct {
	indexPushDowns
	datastore         datastore.Datastore
	systemstore       datastore.Datastore
	namespace         string
	indexApiVersion   int
	featureControls   uint64
	subquery          bool
	correlated        bool
	maxParallelism    int
	delayProjection   bool                  // Used to allow ORDER BY non-projected expressions
	from              algebra.FromTerm      // Used for index selection
	where             expression.Expression // Used for index selection
	filter            expression.Expression // for Filter operator
	setOpDistinct     bool                  // Used for SETOP Distinct to apply DISTINCT on projection
	children          []plan.Operator
	subChildren       []plan.Operator
	cover             expression.HasExpressions
	node              expression.HasExpressions
	coveringScans     []plan.CoveringOperator
	coveredUnnests    map[*algebra.Unnest]bool
	countScan         plan.CoveringOperator
	skipDynamic       bool
	requirePrimaryKey bool
	orderScan         plan.SecondaryScan
	namedArgs         map[string]value.Value
	positionalArgs    value.Values
	baseKeyspaces     map[string]*baseKeyspace
	pushableOnclause  expression.Expression // combined ON-clause from all inner joins
	builderFlags      uint32
}

type indexPushDowns struct {
	order         *algebra.Order        // Used to collect aggregates from ORDER BY, and for ORDER pushdown
	limit         expression.Expression // Used for LIMIT pushdown
	offset        expression.Expression // Used for OFFSET pushdown
	oldAggregates bool                  // Used for pre-API3 Projection aggregate
	projection    *algebra.Projection   // Used for ORDER/projection Distinct pushdown to IndexScan2
	group         *algebra.Group        // Group BY
	aggs          algebra.Aggregates    // all aggregates in query
	aggConstraint expression.Expression // aggregate Constraint
}

func (this *builder) storeIndexPushDowns() *indexPushDowns {
	idxPushDowns := &indexPushDowns{}
	idxPushDowns.order = this.order
	idxPushDowns.limit = this.limit
	idxPushDowns.offset = this.offset
	idxPushDowns.oldAggregates = this.oldAggregates
	idxPushDowns.projection = this.projection
	idxPushDowns.group = this.group
	idxPushDowns.aggs = this.aggs
	idxPushDowns.aggConstraint = this.aggConstraint

	return idxPushDowns
}

func (this *builder) restoreIndexPushDowns(idxPushDowns *indexPushDowns, pagination bool) {
	if pagination {
		this.order = idxPushDowns.order
		this.limit = idxPushDowns.limit
		this.offset = idxPushDowns.offset
	}
	this.oldAggregates = idxPushDowns.oldAggregates
	this.projection = idxPushDowns.projection
	this.group = idxPushDowns.group
	this.aggs = idxPushDowns.aggs
	this.aggConstraint = idxPushDowns.aggConstraint
}

func newBuilder(datastore, systemstore datastore.Datastore, namespace string, subquery bool,
	namedArgs map[string]value.Value, positionalArgs value.Values, indexApiVersion int,
	featureControls uint64) *builder {
	rv := &builder{
		datastore:       datastore,
		systemstore:     systemstore,
		namespace:       namespace,
		subquery:        subquery,
		delayProjection: false,
		namedArgs:       namedArgs,
		positionalArgs:  positionalArgs,
		indexApiVersion: indexApiVersion,
		featureControls: featureControls,
	}

	return rv
}

func (this *builder) trueWhereClause() bool {
	return (this.builderFlags & BUILDER_WHERE_IS_TRUE) != 0
}

func (this *builder) setTrueWhereClause() {
	this.builderFlags |= BUILDER_WHERE_IS_TRUE
}

func (this *builder) unsetTrueWhereClause() {
	this.builderFlags &^= BUILDER_WHERE_IS_TRUE
}

func (this *builder) falseWhereClause() bool {
	return (this.builderFlags & BUILDER_WHERE_IS_FALSE) != 0
}

func (this *builder) setFalseWhereClause() {
	this.builderFlags |= BUILDER_WHERE_IS_FALSE
}

func (this *builder) getTermKeyspace(node *algebra.KeyspaceTerm) (datastore.Keyspace, error) {
	node.SetDefaultNamespace(this.namespace)
	ns := node.Namespace()

	datastore := this.datastore
	if strings.ToLower(ns) == "#system" {
		datastore = this.systemstore
	}

	namespace, err := datastore.NamespaceByName(ns)
	if err != nil {
		return nil, err
	}

	keyspace, err := namespace.KeyspaceByName(node.Keyspace())
	if err != nil {
		return nil, err
	}

	return keyspace, nil
}
