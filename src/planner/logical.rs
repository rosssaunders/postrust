use crate::parser::ast::{
    Expr, GroupByExpr, JoinCondition, JoinExpr, JoinType, OrderByExpr, Query, QueryExpr,
    SelectItem, SelectQuantifier, SelectStatement, SetOperator, SetQuantifier, TableExpression,
    TableFunctionRef, TableRef,
};

#[derive(Debug, Clone)]
pub struct LogicalScan {
    pub table: TableRef,
}

#[derive(Debug, Clone)]
pub struct LogicalFunctionScan {
    pub function: TableFunctionRef,
}

#[derive(Debug, Clone)]
pub struct LogicalJoin {
    pub left: Box<LogicalPlan>,
    pub right: Box<LogicalPlan>,
    pub kind: JoinType,
    pub condition: Option<JoinCondition>,
    pub natural: bool,
}

#[derive(Debug, Clone)]
pub struct LogicalFilter {
    pub predicate: Expr,
    pub input: Box<LogicalPlan>,
}

#[derive(Debug, Clone)]
pub struct LogicalProject {
    pub targets: Vec<SelectItem>,
    pub input: Box<LogicalPlan>,
}

#[derive(Debug, Clone)]
pub struct LogicalAggregate {
    pub group_by: Vec<GroupByExpr>,
    pub having: Option<Expr>,
    pub input: Box<LogicalPlan>,
}

#[derive(Debug, Clone)]
pub struct LogicalSort {
    pub order_by: Vec<OrderByExpr>,
    pub input: Box<LogicalPlan>,
}

#[derive(Debug, Clone)]
pub struct LogicalLimit {
    pub limit: Option<Expr>,
    pub offset: Option<Expr>,
    pub input: Box<LogicalPlan>,
}

#[derive(Debug, Clone)]
pub struct LogicalSetOp {
    pub op: SetOperator,
    pub quantifier: SetQuantifier,
    pub left: Box<LogicalPlan>,
    pub right: Box<LogicalPlan>,
}

#[derive(Debug, Clone)]
pub struct LogicalDistinct {
    pub on: Vec<Expr>,
    pub input: Box<LogicalPlan>,
}

#[derive(Debug, Clone)]
pub struct LogicalSubquery {
    pub plan: Box<LogicalPlan>,
    pub alias: Option<String>,
    pub lateral: bool,
}

#[derive(Debug, Clone)]
pub enum LogicalPlan {
    Result,
    Scan(LogicalScan),
    FunctionScan(LogicalFunctionScan),
    Subquery(LogicalSubquery),
    Join(LogicalJoin),
    Filter(LogicalFilter),
    Aggregate(LogicalAggregate),
    Project(LogicalProject),
    Distinct(LogicalDistinct),
    Sort(LogicalSort),
    Limit(LogicalLimit),
    SetOp(LogicalSetOp),
}

pub fn build_logical_plan(query: &Query) -> LogicalPlan {
    let mut plan = build_query_expr(&query.body);

    if !query.order_by.is_empty() {
        plan = LogicalPlan::Sort(LogicalSort {
            order_by: query.order_by.clone(),
            input: Box::new(plan),
        });
    }

    if query.limit.is_some() || query.offset.is_some() {
        plan = LogicalPlan::Limit(LogicalLimit {
            limit: query.limit.clone(),
            offset: query.offset.clone(),
            input: Box::new(plan),
        });
    }

    plan
}

fn build_query_expr(expr: &QueryExpr) -> LogicalPlan {
    match expr {
        QueryExpr::Select(select) => build_select_plan(select),
        QueryExpr::Nested(inner) => build_logical_plan(inner),
        QueryExpr::SetOperation {
            left,
            op,
            quantifier,
            right,
        } => LogicalPlan::SetOp(LogicalSetOp {
            op: *op,
            quantifier: *quantifier,
            left: Box::new(build_query_expr(left)),
            right: Box::new(build_query_expr(right)),
        }),
    }
}

fn build_select_plan(select: &SelectStatement) -> LogicalPlan {
    let mut plan = if select.from.is_empty() {
        LogicalPlan::Result
    } else {
        build_from_clause(&select.from)
    };

    if let Some(predicate) = &select.where_clause {
        plan = LogicalPlan::Filter(LogicalFilter {
            predicate: predicate.clone(),
            input: Box::new(plan),
        });
    }

    if !select.group_by.is_empty() || select.having.is_some() {
        plan = LogicalPlan::Aggregate(LogicalAggregate {
            group_by: select.group_by.clone(),
            having: select.having.clone(),
            input: Box::new(plan),
        });
    }

    plan = LogicalPlan::Project(LogicalProject {
        targets: select.targets.clone(),
        input: Box::new(plan),
    });

    if matches!(select.quantifier, Some(SelectQuantifier::Distinct))
        || !select.distinct_on.is_empty()
    {
        plan = LogicalPlan::Distinct(LogicalDistinct {
            on: select.distinct_on.clone(),
            input: Box::new(plan),
        });
    }

    plan
}

fn build_from_clause(from: &[TableExpression]) -> LogicalPlan {
    let mut iter = from.iter();
    let Some(first) = iter.next() else {
        return LogicalPlan::Result;
    };
    let mut plan = build_table_expression(first);
    for item in iter {
        let right = build_table_expression(item);
        plan = LogicalPlan::Join(LogicalJoin {
            left: Box::new(plan),
            right: Box::new(right),
            kind: JoinType::Cross,
            condition: None,
            natural: false,
        });
    }
    plan
}

fn build_table_expression(table: &TableExpression) -> LogicalPlan {
    match table {
        TableExpression::Relation(rel) => LogicalPlan::Scan(LogicalScan { table: rel.clone() }),
        TableExpression::Function(function) => LogicalPlan::FunctionScan(LogicalFunctionScan {
            function: function.clone(),
        }),
        TableExpression::Subquery(subquery) => LogicalPlan::Subquery(LogicalSubquery {
            plan: Box::new(build_logical_plan(&subquery.query)),
            alias: subquery.alias.clone(),
            lateral: subquery.lateral,
        }),
        TableExpression::Join(join) => build_join_expression(join),
    }
}

fn build_join_expression(join: &JoinExpr) -> LogicalPlan {
    LogicalPlan::Join(LogicalJoin {
        left: Box::new(build_table_expression(&join.left)),
        right: Box::new(build_table_expression(&join.right)),
        kind: join.kind,
        condition: join.condition.clone(),
        natural: join.natural,
    })
}
