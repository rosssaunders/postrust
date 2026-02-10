use std::future::Future;

use super::{plan, PlanNode};
use crate::catalog::{reset_global_catalog_for_tests, with_global_state_lock};
use crate::parser::sql_parser::parse_statement;
use crate::planner::physical::{PhysicalPlan, ScanType};
use crate::tcop::engine::{execute_planned_query, plan_statement, reset_global_storage_for_tests};

fn block_on<T>(future: impl Future<Output = T>) -> T {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime should start")
        .block_on(future)
}

fn with_isolated_state<T>(f: impl FnOnce() -> T) -> T {
    with_global_state_lock(|| {
        reset_global_catalog_for_tests();
        reset_global_storage_for_tests();
        f()
    })
}

fn run_statement(sql: &str) {
    let statement = parse_statement(sql).expect("statement should parse");
    let planned = plan_statement(statement).expect("statement should plan");
    block_on(execute_planned_query(&planned, &[])).expect("statement should execute");
}

fn extract_scan(plan: &PhysicalPlan) -> &crate::planner::physical::ScanPlan {
    match plan {
        PhysicalPlan::Scan(scan) => scan,
        PhysicalPlan::Project(project) => extract_scan(&project.input),
        PhysicalPlan::Filter(filter) => extract_scan(&filter.input),
        _ => panic!("expected scan plan, got {plan:?}"),
    }
}

fn extract_join(plan: &PhysicalPlan) -> &PhysicalPlan {
    match plan {
        PhysicalPlan::NestedLoopJoin(_) | PhysicalPlan::HashJoin(_) => plan,
        PhysicalPlan::Project(project) => extract_join(&project.input),
        PhysicalPlan::Filter(filter) => extract_join(&filter.input),
        _ => panic!("expected join plan, got {plan:?}"),
    }
}

#[test]
fn plans_index_scan_for_simple_filter() {
    with_isolated_state(|| {
        run_statement("CREATE TABLE t (id int8)");
        run_statement("CREATE INDEX t_id_idx ON t (id)");
        run_statement("INSERT INTO t VALUES (1), (2)");
        let stmt = parse_statement("SELECT * FROM t WHERE id = 1").expect("statement should parse");
        let plan = plan(&stmt);
        match plan {
            PlanNode::Query(query_plan) => {
                let scan = extract_scan(&query_plan.physical);
                assert!(matches!(scan.scan_type, ScanType::Index(_)));
                assert!(scan.filter.is_some());
            }
            other => panic!("expected query plan, got {other:?}"),
        }
    });
}

#[test]
fn plans_nested_loop_join_for_small_inputs() {
    with_isolated_state(|| {
        run_statement("CREATE TABLE a (id int8)");
        run_statement("CREATE TABLE b (id int8)");
        run_statement("INSERT INTO a VALUES (1)");
        run_statement("INSERT INTO b VALUES (1)");
        let stmt =
            parse_statement("SELECT * FROM a JOIN b ON a.id = b.id").expect("statement should parse");
        let plan = plan(&stmt);
        match plan {
            PlanNode::Query(query_plan) => {
                let join = extract_join(&query_plan.physical);
                assert!(matches!(join, PhysicalPlan::NestedLoopJoin(_)));
            }
            other => panic!("expected query plan, got {other:?}"),
        }
    });
}

#[test]
fn falls_back_to_passthrough_for_ctes() {
    let stmt = parse_statement("WITH cte AS (SELECT 1) SELECT * FROM cte")
        .expect("statement should parse");
    let plan = plan(&stmt);
    assert!(matches!(plan, PlanNode::PassThrough(_)));
}
