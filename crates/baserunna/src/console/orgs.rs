//! Organization console pages.

use super::db::{DbList, db_path, db_rows};
use super::{Flash, Page, Pager, Params, Posted, Session, back, esc, field, page, stay, table, url_seg, when};
use crate::meta::Role;
use crate::node::Node;
use axum::Router;
use axum::extract::Path;
use axum::routing::{get, post};
use serde::Deserialize;
use serde_json::json;
use std::sync::Arc;

pub fn routes() -> Router<Arc<Node>> {
    Router::new()
        .route("/console/orgs", get(orgs_page).post(create_org))
        .route("/console/orgs/{org}", get(org_page))
        .route("/console/orgs/{org}/delete", post(delete_org))
        .route("/console/orgs/{org}/leave", post(leave))
        .route("/console/orgs/{org}/dbs", post(create_db))
        .route("/console/orgs/{org}/members", post(add_member))
        .route("/console/orgs/{org}/members/{user}", post(set_role))
        .route("/console/orgs/{org}/members/{user}/remove", post(remove_member))
}

pub(super) fn org_path(org: &str) -> String {
    format!("/console/orgs/{}", url_seg(org))
}

fn api_org(org: &str) -> String {
    format!("/v1/orgs/{}", url_seg(org))
}

#[derive(Deserialize)]
struct Org {
    name: String,
    role: Role,
    created_at: u64,
}

#[derive(Deserialize)]
struct Orgs {
    orgs: Vec<Org>,
    next_cursor: Option<String>,
}

#[derive(Deserialize)]
struct Member {
    user_id: String,
    email: String,
    role: Role,
    added_at: u64,
}

#[derive(Deserialize)]
struct Members {
    members: Vec<Member>,
    next_cursor: Option<String>,
}

fn role_options(selected: Role) -> String {
    Role::ALL
        .iter()
        .map(|r| format!("<option{}>{}</option>", if *r == selected { " selected" } else { "" }, r.name()))
        .collect()
}

async fn orgs_page(s: Session, q: Params) -> Page {
    let p = Pager::of(&q, "cursor");
    let orgs: Orgs = s.get(&format!("/v1/orgs?{}", p.api_query())).await.map_err(|e| s.fail("Organizations", &e))?;
    let rows: Vec<String> = orgs
        .orgs
        .iter()
        .map(|o| {
            format!(
                r#"<tr><td><a href="{p}">{n}</a></td><td>{r}</td><td>{c}</td></tr>"#,
                p = org_path(&o.name),
                n = esc(&o.name),
                r = o.role.name(),
                c = when(o.created_at),
            )
        })
        .collect();
    Ok(page(
        "Organizations",
        Some(&s),
        Flash::of(&q).as_ref(),
        &format!(
            r#"<h1>Organizations</h1><div class="card">{table}{pager}</div>
<div class="card"><form class="row" method="post" action="/console/orgs">{csrf}<input name="name" placeholder="new-organization" pattern="[a-z0-9][a-z0-9_-]*" required><button>Create organization</button></form></div>"#,
            table = table(&["Organization", "Your role", "Created"], &rows, "You do not belong to any organization."),
            pager = p.links("/console/orgs", orgs.next_cursor.as_deref()),
            csrf = s.csrf_input()
        ),
    ))
}

async fn create_org(Posted { s, f }: Posted) -> Page {
    s.act("POST", "/v1/orgs", Some(json!({"name": field(&f, "name")})), "/console/orgs", |j| {
        (org_path(j["name"].as_str().unwrap_or_default()), "Organization created.".into())
    })
    .await
}

async fn org_page(Path(org): Path<String>, s: Session, q: Params) -> Page {
    let info: Org = s.get(&api_org(&org)).await.map_err(|e| back("/console/orgs", &e, false))?;
    let manage = info.role.can_administer();
    let base = org_path(&org);
    let csrf = s.csrf_input();
    let mp = Pager::of(&q, "members_cursor");
    let members: Members =
        s.get(&format!("{}/members?{}", api_org(&org), mp.api_query())).await.map_err(|e| s.fail(&org, &e))?;
    let mrows: Vec<String> = members
        .members
        .iter()
        .map(|m| {
            let mp = format!("{base}/members/{}", url_seg(&m.user_id));
            let actions = if manage {
                format!(
                    r#"<form class="inline" method="post" action="{mp}">{csrf}<select name="role">{opts}</select><button class="link">change role</button></form>{remove}"#,
                    opts = role_options(m.role),
                    remove = s.button(&format!("{mp}/remove"), "remove", true),
                )
            } else {
                String::new()
            };
            format!(
                "<tr><td>{e}{me}</td><td>{r}</td><td>{a}</td><td>{actions}</td></tr>",
                e = esc(&m.email),
                me = if m.user_id == s.user_id { r#" <span class="muted">(you)</span>"# } else { "" },
                r = m.role.name(),
                a = when(m.added_at),
            )
        })
        .collect();
    let dp = Pager::of(&q, "dbs_cursor");
    let dbs: DbList =
        s.get(&format!("/v1/dbs?org={}&{}", url_seg(&org), dp.api_query())).await.map_err(|e| s.fail(&org, &e))?;
    let (add, create_db) = if manage {
        (
            format!(
                r#"<form class="row" method="post" action="{base}/members">{csrf}<input name="email" type="email" placeholder="email of an existing account" required><select name="role">{opts}</select><button>Add member</button></form>"#,
                opts = role_options(Role::Writer),
            ),
            format!(
                r#"<form class="row" method="post" action="{base}/dbs">{csrf}<input name="name" placeholder="new-database" pattern="[a-z0-9][a-z0-9_-]*" required><button>Create database</button></form>"#
            ),
        )
    } else {
        (String::new(), String::new())
    };
    let delete = if info.role == Role::Owner {
        format!(
            r#"<form class="row" method="post" action="{base}/delete">{csrf}<input name="confirm" placeholder="type {on} to confirm"><button class="danger">Delete organization</button></form>
<p class="muted">An organization is deleted once it has no databases.</p>"#,
            on = esc(&org)
        )
    } else {
        String::new()
    };
    Ok(page(
        &org,
        Some(&s),
        Flash::of(&q).as_ref(),
        &format!(
            r#"<p><a href="/console/orgs">← Organizations</a></p><h1>{on}</h1><p class="muted">Your role: {role}</p>
<h2>Databases</h2><div class="card">{dbs}{dpager}{create_db}</div>
<h2>Members</h2><div class="card">{members}{mpager}{add}</div>
<h2>Membership</h2><div class="card"><form class="row" method="post" action="{base}/leave">{csrf}<button class="danger">Leave organization</button></form>{delete}</div>"#,
            on = esc(&org),
            role = info.role.name(),
            dbs = db_rows(&dbs.databases, Some(&org)),
            dpager = dp.links(&base, dbs.next_cursor.as_deref()),
            members = table(&["Member", "Role", "Added", ""], &mrows, "No members."),
            mpager = mp.links(&base, members.next_cursor.as_deref()),
        ),
    ))
}

async fn delete_org(Path(org): Path<String>, Posted { s, f }: Posted) -> Page {
    if field(&f, "confirm") != org {
        return Ok(back(&org_path(&org), "Type the organization name to confirm.", false));
    }
    s.act("DELETE", &api_org(&org), None, &org_path(&org), stay("/console/orgs", "Organization deleted.")).await
}

async fn leave(Path(org): Path<String>, Posted { s, .. }: Posted) -> Page {
    let path = format!("{}/members/{}", api_org(&org), url_seg(&s.user_id));
    s.act("DELETE", &path, None, &org_path(&org), stay("/console/orgs", "You left the organization.")).await
}

async fn create_db(Path(org): Path<String>, Posted { s, f }: Posted) -> Page {
    let body = json!({"name": field(&f, "name"), "org": org});
    s.act("POST", "/v1/dbs", Some(body), &org_path(&org), |j| {
        let name = j["name"].as_str().unwrap_or_default();
        (db_path(&crate::meta::DbRef::format(Some(&org), name)), "Database created.".into())
    })
    .await
}

async fn add_member(Path(org): Path<String>, Posted { s, f }: Posted) -> Page {
    let body = json!({"email": field(&f, "email"), "role": field(&f, "role")});
    let op = org_path(&org);
    s.act("POST", &format!("{}/members", api_org(&org)), Some(body), &op, stay(&op, "Member added.")).await
}

async fn set_role(Path((org, user)): Path<(String, String)>, Posted { s, f }: Posted) -> Page {
    let path = format!("{}/members/{}", api_org(&org), url_seg(&user));
    let op = org_path(&org);
    s.act("POST", &path, Some(json!({"role": field(&f, "role")})), &op, stay(&op, "Role changed.")).await
}

async fn remove_member(Path((org, user)): Path<(String, String)>, Posted { s, .. }: Posted) -> Page {
    let path = format!("{}/members/{}", api_org(&org), url_seg(&user));
    let op = org_path(&org);
    s.act("DELETE", &path, None, &op, stay(&op, "Member removed.")).await
}
