// reproduces the differential corpus's newly-enabled windowed shapes (GAP-2)
// plus the unbounded allProjects, all desired together in ONE client group and
// pulled through handle_query_pull, to catch a windowed-query recompute error or
// a multi-query interaction that would stall the real differential.
mod common;

use common::TestDb;
use serde_json::{Value, json};

use sync_core::query::{handle_query_pull, init_query_schema};
use sync_core::{SyncDb, Tables, Transactor, init_schema};

fn tables() -> Tables {
    Tables::from_zero_schema(&json!({
        "tables": {
            "project": {
                "serverName": "project_record",
                "columns": {
                    "id": { "type": "string", "serverName": "project_id" },
                    "ownerId": { "type": "string", "serverName": "owner_id" },
                    "name": { "type": "string", "serverName": "project_name" },
                },
                "primaryKey": ["id"],
            },
            "member": {
                "serverName": "project_member",
                "columns": {
                    "id": { "type": "string", "serverName": "member_id" },
                    "projectId": { "type": "string", "serverName": "project_id" },
                    "userId": { "type": "string", "serverName": "user_id" },
                },
                "primaryKey": ["id"],
            },
            "task": {
                "serverName": "project_task",
                "columns": {
                    "id": { "type": "string", "serverName": "task_id" },
                    "projectId": { "type": "string", "serverName": "project_id" },
                    "rank": { "type": "number", "serverName": "sort_rank" },
                },
                "primaryKey": ["id"],
            },
            "user": {
                "serverName": "user_record",
                "columns": {
                    "id": { "type": "string", "serverName": "user_id" },
                    "name": { "type": "string", "serverName": "display_name" },
                },
                "primaryKey": ["id"],
            },
        }
    }))
    .unwrap()
}

fn setup() -> TestDb {
    let mut db = TestDb::memory();
    for ddl in [
        "CREATE TABLE project_record (project_id TEXT PRIMARY KEY, owner_id TEXT, project_name TEXT)",
        "CREATE TABLE project_member (member_id TEXT PRIMARY KEY, project_id TEXT, user_id TEXT)",
        "CREATE TABLE project_task (task_id TEXT PRIMARY KEY, project_id TEXT, sort_rank REAL)",
        "CREATE TABLE user_record (user_id TEXT PRIMARY KEY, display_name TEXT)",
    ] {
        db.exec(ddl, &[]).unwrap();
    }
    db.exec(
        "INSERT INTO project_record VALUES ('p0','u0','A'), ('p1','u1','B'), ('p2','u0','C')",
        &[],
    )
    .unwrap();
    db.exec(
        "INSERT INTO project_member VALUES ('mb0','p0','u0'), ('mb1','p0','u1'), ('mb2','p2','u0')",
        &[],
    )
    .unwrap();
    // p0 has 3 tasks (ranks 1,2,3); p2 has 1 task
    db.exec(
        "INSERT INTO project_task VALUES ('t0','p0',1),('t1','p0',2),('t2','p0',3),('t3','p2',5)",
        &[],
    )
    .unwrap();
    db.exec(
        "INSERT INTO user_record VALUES ('u0','U0'),('u1','U1')",
        &[],
    )
    .unwrap();
    let t = tables();
    init_schema(&mut db, &t).unwrap();
    init_query_schema(&mut db).unwrap();
    db
}

fn put_ids(resp: &Value, table: &str) -> Vec<String> {
    let mut v: Vec<String> = resp["rowsPatch"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|op| op["op"] == "put" && op["tableName"] == table)
        .map(|op| op["value"]["id"].as_str().unwrap().to_string())
        .collect();
    v.sort();
    v
}

// the transformed ASTs matching the harness query builders
fn all_projects() -> Value {
    json!({ "table": "project", "related": [
        { "correlation": { "parentField": ["id"], "childField": ["projectId"] },
          "subquery": { "table": "member" } }] })
}
fn projects_with_recent_tasks() -> Value {
    json!({ "table": "project", "related": [
        { "correlation": { "parentField": ["id"], "childField": ["projectId"] },
          "subquery": { "table": "task", "orderBy": [["rank", "desc"]], "limit": 3 } }] })
}
fn project_tasks_page() -> Value {
    json!({ "table": "project", "orderBy": [["name", "asc"]], "limit": 4, "related": [
        { "correlation": { "parentField": ["id"], "childField": ["projectId"] },
          "subquery": { "table": "task", "orderBy": [["rank", "desc"]], "limit": 2 } },
        { "correlation": { "parentField": ["id"], "childField": ["projectId"] },
          "subquery": { "table": "member", "related": [
            { "correlation": { "parentField": ["userId"], "childField": ["id"] },
              "subquery": { "table": "user", "limit": 1 } }] } }] })
}
// projectById(p0): root .one() (limit 1) + members(-> user .one()) + tasks(orderBy, unbounded)
fn project_by_id() -> Value {
    json!({ "table": "project", "limit": 1,
        "where": { "type": "simple", "op": "=", "left": { "type": "column", "name": "id" },
                   "right": { "type": "literal", "value": "p0" } },
        "related": [
        { "correlation": { "parentField": ["id"], "childField": ["projectId"] },
          "subquery": { "table": "member", "related": [
            { "correlation": { "parentField": ["userId"], "childField": ["id"] },
              "subquery": { "table": "user", "limit": 1 } }] } },
        { "correlation": { "parentField": ["id"], "childField": ["projectId"] },
          "subquery": { "table": "task", "orderBy": [["rank", "desc"]] } }] })
}
// projectMemberUsers: project orderBy id, members(-> user .one())
fn project_member_users() -> Value {
    json!({ "table": "project", "orderBy": [["id", "asc"]], "related": [
        { "correlation": { "parentField": ["id"], "childField": ["projectId"] },
          "subquery": { "table": "member", "related": [
            { "correlation": { "parentField": ["userId"], "childField": ["id"] },
              "subquery": { "table": "user", "limit": 1 } }] } }] })
}
// membersOfProject(p0): ROOT is member, orderBy id, related user .one()
fn members_of_project() -> Value {
    json!({ "table": "member", "orderBy": [["id", "asc"]],
        "where": { "type": "simple", "op": "=", "left": { "type": "column", "name": "projectId" },
                   "right": { "type": "literal", "value": "p0" } },
        "related": [
        { "correlation": { "parentField": ["userId"], "childField": ["id"] },
          "subquery": { "table": "user", "limit": 1 } }] })
}

#[test]
fn windowed_corpus_and_all_projects_pull_together() {
    let mut db = setup();
    let t = tables();
    let patch: Vec<Value> = vec![
        json!({ "op": "put", "hash": "allProjects", "ast": all_projects() }),
        json!({ "op": "put", "hash": "projectsWithRecentTasks", "ast": projects_with_recent_tasks() }),
        json!({ "op": "put", "hash": "projectTasksPage", "ast": project_tasks_page() }),
        json!({ "op": "put", "hash": "projectById", "ast": project_by_id() }),
        json!({ "op": "put", "hash": "projectMemberUsers", "ast": project_member_users() }),
        json!({ "op": "put", "hash": "membersOfProject", "ast": members_of_project() }),
    ];
    let body = json!({ "clientID": "c", "clientGroupID": "g", "cookie": null,
        "queries": { "version": 1, "patch": patch } });
    // must NOT error and must NOT stall: a fresh pull returns every query's rows.
    let resp = db
        .transaction(|d| handle_query_pull(d, &t, 4096, &body, "u"))
        .unwrap();

    // allProjects: all projects + all members present (unbounded child survives
    // alongside the windowed queries).
    assert_eq!(put_ids(&resp, "project"), vec!["p0", "p1", "p2"]);
    assert_eq!(put_ids(&resp, "member"), vec!["mb0", "mb1", "mb2"]);
    // windowed tasks: projectsWithRecentTasks (top-3 of p0's 3) + projectTasksPage
    // (top-2 of p0) union to p0's t0,t1,t2 and p2's t3 -> all 4 tasks appear
    // (t0 enters via the top-3 window even though it is p0's lowest-ranked).
    assert_eq!(put_ids(&resp, "task"), vec!["t0", "t1", "t2", "t3"]);
    // nested .one() users under members (projectTasksPage)
    assert_eq!(put_ids(&resp, "user"), vec!["u0", "u1"]);
    assert_eq!(resp["gotQueries"]["version"], json!(1));

    // physical writes are journaled under logical table/pk keys, then the
    // mapped incremental compiler re-emits logical row objects.
    let cookie = resp["cookie"].clone();
    db.exec(
        "UPDATE project_record SET project_name = 'Z' WHERE project_id = 'p0'",
        &[],
    )
    .unwrap();
    db.exec("INSERT INTO project_task VALUES ('t4', 'p0', 10)", &[])
        .unwrap();
    let body2 = json!({ "clientID": "c", "clientGroupID": "g", "cookie": cookie });
    let resp2 = db
        .transaction(|d| handle_query_pull(d, &t, 4096, &body2, "u"))
        .unwrap();
    assert!(resp2["rowsPatch"].as_array().unwrap().iter().any(|op| {
        op["op"] == "put"
            && op["tableName"] == "project"
            && op["value"]["id"] == "p0"
            && op["value"]["name"] == "Z"
    }));
    assert_eq!(put_ids(&resp2, "task"), vec!["t4"]);

    let caught_up = json!({
        "clientID": "c",
        "clientGroupID": "g",
        "cookie": resp2["cookie"].clone(),
    });
    let resp3 = db
        .transaction(|d| handle_query_pull(d, &t, 4096, &caught_up, "u"))
        .unwrap();
    assert_eq!(resp3["unchanged"], json!(true));
}
