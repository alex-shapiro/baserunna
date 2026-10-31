//! Organization and membership management routes.

use super::{Auth, Endpoints as _, JsonBody, Params, Reply, Req, paging};
use crate::error::{Error, Result};
use crate::meta::{self, MemberInfo, MemberLimits, MetaCmd, ORG_ID_PREFIX, Org, Role};
use crate::node::Node;
use axum::Router;
use axum::extract::{Path, State};
use baserunna_api::{self as api, Endpoint as _};

use axum::routing::{get, post};
use serde::{Deserialize, Serialize};
use serde_json::{Value as Json, json};
use std::sync::Arc;

type J = axum::Json<Json>;

pub fn routes() -> Router<Arc<Node>> {
    Router::new()
        .route(api::CreateOrg::ROUTE, get(list_orgs))
        .endpoint::<api::CreateOrg, _>(create_org)
        .route("/v1/orgs/{org}", get(get_org).delete(delete_org))
        .route(api::AddMember::ROUTE, get(list_members))
        .endpoint::<api::AddMember, _>(add_member)
        .route(api::RemoveMember::ROUTE, post(set_role))
        .endpoint::<api::RemoveMember, _>(remove_member)
}

#[derive(Serialize)]
struct OrgView {
    #[serde(flatten)]
    org: Org,
    role: Role,
}

fn to_json<T: Serialize>(v: &T) -> Result<Json> {
    serde_json::to_value(v).map_err(|e| Error::internal(e.to_string()))
}

async fn list_orgs(State(node): State<Arc<Node>>, Auth(p): Auth, q: Params) -> Result<J> {
    p.require_unscoped()?;
    let (limit, cursor) = paging(&q)?;
    let mut t = node.sys_read_strong().await?;
    let page = meta::memberships_page(&mut t, &p.user.id, limit, cursor).await?;
    let mut orgs = Vec::with_capacity(page.items.len());
    for m in page.items {
        let org = meta::get_org(&mut t, &m.org_id).await?;
        let org = org.ok_or_else(|| Error::internal(format!("membership of missing organization {}", m.org_id)))?;
        orgs.push(OrgView { org, role: m.role });
    }
    Ok(axum::Json(json!({"orgs": to_json(&orgs)?, "next_cursor": page.next_cursor})))
}

async fn create_org(
    State(node): State<Arc<Node>>,
    Auth(p): Auth,
    Req(c): Req<api::CreateOrg>,
) -> Result<Reply<api::CreateOrg>> {
    p.require_session()?;
    let org: Org = node
        .run_meta_as(MetaCmd::CreateOrg {
            id: format!("{ORG_ID_PREFIX}{}", node.env.random_hex(8)),
            name: c.name,
            actor: p.user.id.clone(),
            max_orgs: node.cfg.max_orgs_per_user,
        })
        .await?;
    Ok(Reply::new(api::Org { id: org.id, name: org.name, created_at: org.created_at, role: Role::Owner.into() }))
}

async fn get_org(State(node): State<Arc<Node>>, Auth(p): Auth, Path(name): Path<String>) -> Result<J> {
    p.require_unscoped()?;
    let (org, role) = node.resolve_org(&p, &name).await?;
    Ok(axum::Json(to_json(&OrgView { org, role })?))
}

async fn delete_org(State(node): State<Arc<Node>>, Auth(p): Auth, Path(name): Path<String>) -> Result<J> {
    p.require_session()?;
    let (o, _) = node.resolve_org(&p, &name).await?;
    node.run_meta(MetaCmd::DeleteOrg { org_id: o.id, actor: p.user.id.clone() }).await?;
    node.forget_org(&name);
    Ok(axum::Json(json!({})))
}

async fn list_members(State(node): State<Arc<Node>>, Auth(p): Auth, Path(name): Path<String>, q: Params) -> Result<J> {
    p.require_unscoped()?;
    let (limit, cursor) = paging(&q)?;
    let (o, _) = node.resolve_org(&p, &name).await?;
    let mut t = node.sys_read_strong().await?;
    let page = meta::members_page(&mut t, &o.id, limit, cursor).await?;
    let mut members = Vec::with_capacity(page.items.len());
    for m in page.items {
        members.push(MemberInfo::of(&mut t, m).await?);
    }
    Ok(axum::Json(json!({"members": to_json(&members)?, "next_cursor": page.next_cursor})))
}

async fn add_member(
    State(node): State<Arc<Node>>,
    Auth(p): Auth,
    Path(name): Path<String>,
    Req(b): Req<api::AddMember>,
) -> Result<Reply<api::AddMember>> {
    p.require_session()?;
    let email = crate::auth::normalize_email(&b.email)?;
    let (o, _) = node.resolve_org(&p, &name).await?;
    let limits = MemberLimits { max_members: node.cfg.max_members_per_org, max_orgs: node.cfg.max_orgs_per_user };
    let cmd = MetaCmd::AddMember { org_id: o.id, actor: p.user.id.clone(), email, role: b.role.into(), limits };
    let m: MemberInfo = node.run_meta_as(cmd).await?;
    Ok(Reply::new(api::Member { user_id: m.user_id, email: m.email, role: m.role.into(), added_at: m.added_at }))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SetRole {
    role: Role,
}

async fn set_role(
    State(node): State<Arc<Node>>,
    Auth(p): Auth,
    Path((name, user)): Path<(String, String)>,
    JsonBody(b): JsonBody<SetRole>,
) -> Result<J> {
    p.require_session()?;
    let (o, _) = node.resolve_org(&p, &name).await?;
    let cmd = MetaCmd::SetRole { org_id: o.id.clone(), actor: p.user.id.clone(), user_id: user.clone(), role: b.role };
    let m: MemberInfo = node.run_meta_as(cmd).await?;
    node.forget_role(&o.id, &user);
    Ok(axum::Json(to_json(&m)?))
}

async fn remove_member(
    State(node): State<Arc<Node>>,
    Auth(p): Auth,
    Path((name, user)): Path<(String, String)>,
    _: Req<api::RemoveMember>,
) -> Result<Reply<api::RemoveMember>> {
    p.require_session()?;
    let (o, _) = node.resolve_org(&p, &name).await?;
    node.run_meta(MetaCmd::RemoveMember { org_id: o.id.clone(), actor: p.user.id.clone(), user_id: user.clone() })
        .await?;
    node.forget_role(&o.id, &user);
    Ok(Reply::new(api::Empty {}))
}
