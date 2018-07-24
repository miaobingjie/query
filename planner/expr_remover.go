//  Copyright (c) 2018 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package planner

import (
	"github.com/couchbase/query/expression"
)

// Remove an sub-expression from an expression
func RemoveExpr(expr, removeExpr expression.Expression) (expression.Expression, error) {
	if expr == nil || removeExpr == nil {
		return nil, nil
	}

	remover := newExprRemover(removeExpr)
	newExpr, err := expr.Accept(remover)
	if err != nil {
		return nil, err
	}

	if newExpr == nil {
		return nil, nil
	}
	return newExpr.(expression.Expression), nil
}

type exprRemover struct {
	removeExpr expression.Expression
}

func newExprRemover(removeExpr expression.Expression) *exprRemover {
	return &exprRemover{
		removeExpr: removeExpr,
	}
}

/*
Only remove expression on AND boundary
*/
func (this *exprRemover) VisitAnd(expr *expression.And) (interface{}, error) {
	and, _ := flattenAnd(expr)
	terms := make(expression.Expressions, 0, len(and.Operands()))
	for _, op := range and.Operands() {
		sub, err := this.visitDefault(op)
		if err != nil {
			return nil, err
		}
		if sub != nil {
			terms = append(terms, sub.(expression.Expression))
		}
	}

	if len(terms) == 0 {
		return nil, nil
	}
	return expression.NewAnd(terms...), nil
}

// Arithmetic

func (this *exprRemover) VisitAdd(pred *expression.Add) (interface{}, error) {
	return this.visitDefault(pred)
}

func (this *exprRemover) VisitDiv(pred *expression.Div) (interface{}, error) {
	return this.visitDefault(pred)
}

func (this *exprRemover) VisitMod(pred *expression.Mod) (interface{}, error) {
	return this.visitDefault(pred)
}

func (this *exprRemover) VisitMult(pred *expression.Mult) (interface{}, error) {
	return this.visitDefault(pred)
}

func (this *exprRemover) VisitNeg(pred *expression.Neg) (interface{}, error) {
	return this.visitDefault(pred)
}

func (this *exprRemover) VisitSub(pred *expression.Sub) (interface{}, error) {
	return this.visitDefault(pred)
}

// Case

func (this *exprRemover) VisitSearchedCase(pred *expression.SearchedCase) (interface{}, error) {
	return this.visitDefault(pred)
}

func (this *exprRemover) VisitSimpleCase(pred *expression.SimpleCase) (interface{}, error) {
	return this.visitDefault(pred)
}

// Collection

func (this *exprRemover) VisitAny(pred *expression.Any) (interface{}, error) {
	return this.visitDefault(pred)
}

func (this *exprRemover) VisitEvery(pred *expression.Every) (interface{}, error) {
	return this.visitDefault(pred)
}

func (this *exprRemover) VisitAnyEvery(pred *expression.AnyEvery) (interface{}, error) {
	return this.visitDefault(pred)
}

func (this *exprRemover) VisitArray(pred *expression.Array) (interface{}, error) {
	return this.visitDefault(pred)
}

func (this *exprRemover) VisitFirst(pred *expression.First) (interface{}, error) {
	return this.visitDefault(pred)
}

func (this *exprRemover) VisitObject(pred *expression.Object) (interface{}, error) {
	return this.visitDefault(pred)
}

func (this *exprRemover) VisitExists(pred *expression.Exists) (interface{}, error) {
	return this.visitDefault(pred)
}

func (this *exprRemover) VisitIn(pred *expression.In) (interface{}, error) {
	return this.visitDefault(pred)
}

func (this *exprRemover) VisitWithin(pred *expression.Within) (interface{}, error) {
	return this.visitDefault(pred)
}

// Comparison

func (this *exprRemover) VisitBetween(pred *expression.Between) (interface{}, error) {
	return this.visitDefault(pred)
}

func (this *exprRemover) VisitEq(pred *expression.Eq) (interface{}, error) {
	return this.visitDefault(pred)
}

func (this *exprRemover) VisitLE(pred *expression.LE) (interface{}, error) {
	return this.visitDefault(pred)
}

func (this *exprRemover) VisitLike(pred *expression.Like) (interface{}, error) {
	return this.visitDefault(pred)
}

func (this *exprRemover) VisitLT(pred *expression.LT) (interface{}, error) {
	return this.visitDefault(pred)
}

func (this *exprRemover) VisitIsMissing(pred *expression.IsMissing) (interface{}, error) {
	return this.visitDefault(pred)
}

func (this *exprRemover) VisitIsNotMissing(pred *expression.IsNotMissing) (interface{}, error) {
	return this.visitDefault(pred)
}

func (this *exprRemover) VisitIsNotNull(pred *expression.IsNotNull) (interface{}, error) {
	return this.visitDefault(pred)
}

func (this *exprRemover) VisitIsNotValued(pred *expression.IsNotValued) (interface{}, error) {
	return this.visitDefault(pred)
}

func (this *exprRemover) VisitIsNull(pred *expression.IsNull) (interface{}, error) {
	return this.visitDefault(pred)
}

func (this *exprRemover) VisitIsValued(pred *expression.IsValued) (interface{}, error) {
	return this.visitDefault(pred)
}

// Concat
func (this *exprRemover) VisitConcat(pred *expression.Concat) (interface{}, error) {
	return this.visitDefault(pred)
}

// Constant
func (this *exprRemover) VisitConstant(pred *expression.Constant) (interface{}, error) {
	return this.visitDefault(pred)
}

// Identifier
func (this *exprRemover) VisitIdentifier(pred *expression.Identifier) (interface{}, error) {
	return this.visitDefault(pred)
}

// Construction

func (this *exprRemover) VisitArrayConstruct(pred *expression.ArrayConstruct) (interface{}, error) {
	return this.visitDefault(pred)
}

func (this *exprRemover) VisitObjectConstruct(pred *expression.ObjectConstruct) (interface{}, error) {
	return this.visitDefault(pred)
}

// Logic

func (this *exprRemover) VisitOr(pred *expression.Or) (interface{}, error) {
	return this.visitDefault(pred)
}

func (this *exprRemover) VisitNot(pred *expression.Not) (interface{}, error) {
	return this.visitDefault(pred)
}

// Navigation

func (this *exprRemover) VisitElement(pred *expression.Element) (interface{}, error) {
	return this.visitDefault(pred)
}

func (this *exprRemover) VisitField(pred *expression.Field) (interface{}, error) {
	return this.visitDefault(pred)
}

func (this *exprRemover) VisitFieldName(pred *expression.FieldName) (interface{}, error) {
	return this.visitDefault(pred)
}

func (this *exprRemover) VisitSlice(pred *expression.Slice) (interface{}, error) {
	return this.visitDefault(pred)
}

// Self
func (this *exprRemover) VisitSelf(pred *expression.Self) (interface{}, error) {
	return this.visitDefault(pred)
}

// Function
func (this *exprRemover) VisitFunction(pred expression.Function) (interface{}, error) {
	return this.visitDefault(pred)
}

// Subquery
func (this *exprRemover) VisitSubquery(pred expression.Subquery) (interface{}, error) {
	return this.visitDefault(pred)
}

// NamedParameter
func (this *exprRemover) VisitNamedParameter(pred expression.NamedParameter) (interface{}, error) {
	return this.visitDefault(pred)
}

// PositionalParameter
func (this *exprRemover) VisitPositionalParameter(pred expression.PositionalParameter) (interface{}, error) {
	return this.visitDefault(pred)
}

// Cover
func (this *exprRemover) VisitCover(pred *expression.Cover) (interface{}, error) {
	return this.visitDefault(pred)
}

// All
func (this *exprRemover) VisitAll(pred *expression.All) (interface{}, error) {
	return this.visitDefault(pred)
}

func (this *exprRemover) visitDefault(expr expression.Expression) (interface{}, error) {
	if expr.EquivalentTo(this.removeExpr) {
		return nil, nil
	}
	return expr, nil
}
