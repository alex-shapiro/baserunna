//! Organizations: databases shared by several users, each a member with a
//! role. See SPEC §9.
//!
//! | role     | data (read) | data (write, schema) | create/delete databases | manage members        | delete org |
//! | -------- | ----------- | -------------------- | ----------------------- | --------------------- | ---------- |
//! | `reader` | yes         |                      |                         |                       |            |
//! | `writer` | yes         | yes                  |                         |                       |            |
//! | `admin`  | yes         | yes                  | yes                     | readers to admins     |            |
//! | `owner`  | yes         | yes                  | yes                     | everyone              | yes        |
//!
//! Every organization keeps at least one owner. Enabled site administrators
//! act as owners of every organization.

use super::{
    Count, Page, User, Write, active_user, add_count, count, first, from_row, get_user, page, q_eq, query_all, to_row,
};
use crate::engine::Txn;
use crate::error::{Error, Result};
use crate::table::{self, InsertMode};
use serde::{Deserialize, Serialize};
use serde_json::json;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Role {
    Reader,
    Writer,
    Admin,
    Owner,
}

impl From<Role> for baserunna_api::Role {
    fn from(r: Role) -> baserunna_api::Role {
        match r {
            Role::Reader => baserunna_api::Role::Reader,
            Role::Writer => baserunna_api::Role::Writer,
            Role::Admin => baserunna_api::Role::Admin,
            Role::Owner => baserunna_api::Role::Owner,
        }
    }
}

impl From<baserunna_api::Role> for Role {
    fn from(r: baserunna_api::Role) -> Role {
        match r {
            baserunna_api::Role::Reader => Role::Reader,
            baserunna_api::Role::Writer => Role::Writer,
            baserunna_api::Role::Admin => Role::Admin,
            baserunna_api::Role::Owner => Role::Owner,
        }
    }
}

impl Role {
    pub const ALL: [Role; 4] = [Role::Reader, Role::Writer, Role::Admin, Role::Owner];

    pub fn name(self) -> &'static str {
        match self {
            Role::Reader => "reader",
            Role::Writer => "writer",
            Role::Admin => "admin",
            Role::Owner => "owner",
        }
    }

    pub fn can_write(self) -> bool {
        self >= Role::Writer
    }

    pub fn can_administer(self) -> bool {
        self >= Role::Admin
    }

    /// Returns whether a member with role `self` may change a user's membership
    /// from `from` to `to`. Only owners may add, modify, or remove owners.
    pub fn may_change(self, from: Option<Role>, to: Option<Role>) -> bool {
        let owners_involved = from == Some(Role::Owner) || to == Some(Role::Owner);
        self.can_administer() && (!owners_involved || self == Role::Owner)
    }

    /// Returns `Some(Role::Owner)` if `user` is an enabled site administrator.
    pub fn of_site_admin(user: &User) -> Option<Role> {
        (user.is_admin && !user.disabled).then_some(Role::Owner)
    }
}

/// Effective permissions of a principal on a database.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Access {
    pub role: Role,
    pub read_only_key: bool,
}

impl Access {
    pub fn can_write(&self) -> bool {
        self.role.can_write() && !self.read_only_key
    }

    pub fn require_write(&self) -> Result<()> {
        if self.read_only_key {
            return Err(Error::PermissionDenied("this credential is read-only".into()));
        }
        if !self.role.can_write() {
            return Err(Error::PermissionDenied(format!("the {} role cannot write", self.role.name())));
        }
        Ok(())
    }

    pub fn require_admin(&self) -> Result<()> {
        self.require_write()?;
        if !self.role.can_administer() {
            return Err(Error::PermissionDenied("this requires the admin role".into()));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Org {
    pub id: String,
    pub name: String,
    pub created_at: i64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Member {
    pub org_id: String,
    pub user_id: String,
    pub role: Role,
    pub added_at: i64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MemberInfo {
    pub user_id: String,
    pub email: String,
    pub role: Role,
    pub added_at: i64,
}

impl MemberInfo {
    pub async fn of(txn: &mut Txn, m: Member) -> Result<MemberInfo> {
        let user = get_user(txn, &m.user_id).await?;
        let email = user.ok_or_else(|| Error::internal(format!("member {} has no account", m.user_id)))?.email;
        Ok(MemberInfo { user_id: m.user_id, email, role: m.role, added_at: m.added_at })
    }
}

/// Returns whether `s` is 1..=39 characters of `[a-z0-9_-]` starting with an alphanumeric character.
pub fn valid_org_name(s: &str) -> bool {
    super::valid_slug(s, ORG_NAME_MAX)
}

pub const ORG_NAME_MAX: usize = 39;

pub async fn get_org(txn: &mut Txn, id: &str) -> Result<Option<Org>> {
    table::get(txn, "orgs", &json!(id)).await?.map(from_row).transpose()
}

pub async fn org_by_name(txn: &mut Txn, name: &str) -> Result<Option<Org>> {
    first(txn, "orgs", "by_name", vec![json!(name)]).await
}

async fn get_member(txn: &mut Txn, org_id: &str, user_id: &str) -> Result<Option<Member>> {
    table::get(txn, "org_members", &json!([org_id, user_id])).await?.map(from_row).transpose()
}

pub async fn member_role(txn: &mut Txn, org_id: &str, user_id: &str) -> Result<Option<Role>> {
    Ok(get_member(txn, org_id, user_id).await?.map(|m| m.role))
}

pub async fn memberships_page(
    txn: &mut Txn,
    user_id: &str,
    limit: usize,
    cursor: Option<String>,
) -> Result<Page<Member>> {
    page(txn, "org_members", q_eq(Some("by_user"), vec![json!(user_id)]), limit, cursor).await
}

pub async fn members_page(txn: &mut Txn, org_id: &str, limit: usize, cursor: Option<String>) -> Result<Page<Member>> {
    page(txn, "org_members", q_eq(None, vec![json!(org_id)]), limit, cursor).await
}

pub(super) async fn memberships(txn: &mut Txn, user_id: &str) -> Result<Vec<Member>> {
    query_all(txn, "org_members", q_eq(Some("by_user"), vec![json!(user_id)])).await
}

async fn acting_role(txn: &mut Txn, org_id: &str, actor: &User) -> Result<Option<Role>> {
    match Role::of_site_admin(actor) {
        Some(r) => Ok(Some(r)),
        None => member_role(txn, org_id, &actor.id).await,
    }
}

fn org_not_found() -> Error {
    Error::not_found("organization not found")
}

/// Verifies that `org_id` exists and `actor` has at least `min` role, returning
/// `NotFound` to non-members.
pub(super) async fn require_role(txn: &mut Txn, org_id: &str, actor: &User, min: Role) -> Result<Role> {
    if get_org(txn, org_id).await?.is_none() {
        return Err(org_not_found());
    }
    match acting_role(txn, org_id, actor).await? {
        None => Err(org_not_found()),
        Some(r) if r < min => Err(Error::PermissionDenied(format!("this requires the {} role", min.name()))),
        Some(r) => Ok(r),
    }
}

/// Updates `user_id`'s membership in `org_id` from `from` to `to` and adjusts
/// materialized counters.
pub(super) async fn change_membership(
    w: &mut Write<'_, '_>,
    org_id: &str,
    user_id: &str,
    from: Option<Role>,
    to: Option<(Role, i64)>,
) -> Result<()> {
    match to {
        Some((role, added_at)) => {
            let m = Member { org_id: org_id.into(), user_id: user_id.into(), role, added_at };
            table::insert(w.txn, w.blobs, "org_members", &to_row(&m), InsertMode::Upsert).await?;
        }
        None => {
            table::delete(w.txn, "org_members", &json!([org_id, user_id])).await?;
        }
    }
    let joined = from.is_none() as i64 - to.is_none() as i64;
    if joined != 0 {
        add_count(w.txn, w.blobs, Count::Members, org_id, joined).await?;
        add_count(w.txn, w.blobs, Count::Orgs, user_id, joined).await?;
    }
    let owners = (to.map(|t| t.0) == Some(Role::Owner)) as i64 - (from == Some(Role::Owner)) as i64;
    if owners != 0 {
        add_count(w.txn, w.blobs, Count::Owners, org_id, owners).await?;
    }
    Ok(())
}

async fn keep_an_owner(txn: &mut Txn, org_id: &str, from: Role, to: Option<Role>) -> Result<()> {
    if from == Role::Owner && to != Some(Role::Owner) && count(txn, Count::Owners, org_id).await? <= 1 {
        return Err(Error::PermissionDenied("an organization needs at least one owner".into()));
    }
    Ok(())
}

async fn org_slot(txn: &mut Txn, user_id: &str, max_orgs: usize) -> Result<()> {
    if count(txn, Count::Orgs, user_id).await? >= max_orgs as i64 {
        return Err(Error::PermissionDenied(format!("a user can belong to at most {max_orgs} organizations")));
    }
    Ok(())
}

pub(super) async fn create_org(
    w: &mut Write<'_, '_>,
    id: &str,
    name: &str,
    actor: &str,
    max_orgs: usize,
) -> Result<Org> {
    if !valid_org_name(name) {
        return Err(Error::invalid(format!(
            "organization names are 1-{ORG_NAME_MAX} chars of [a-z0-9_-], starting alphanumeric"
        )));
    }
    if !super::is_org_id(id) {
        return Err(Error::internal("organization ids carry the organization prefix"));
    }
    let me = active_user(w.txn, actor).await?;
    org_slot(w.txn, &me.id, max_orgs).await?;
    let o = Org { id: id.into(), name: name.into(), created_at: w.now_ms };
    table::insert(w.txn, w.blobs, "orgs", &to_row(&o), InsertMode::Insert).await.map_err(|e| match e {
        Error::AlreadyExists(_) => Error::AlreadyExists(format!("organization {name:?} already exists")),
        other => other,
    })?;
    change_membership(w, id, &me.id, None, Some((Role::Owner, w.now_ms))).await?;
    Ok(o)
}

pub(super) async fn delete_org(w: &mut Write<'_, '_>, org_id: &str, actor: &str) -> Result<()> {
    let me = active_user(w.txn, actor).await?;
    require_role(w.txn, org_id, &me, Role::Owner).await?;
    if count(w.txn, Count::Dbs, org_id).await? > 0 {
        return Err(Error::PermissionDenied("delete the organization's databases first".into()));
    }
    let members: Vec<Member> = query_all(w.txn, "org_members", q_eq(None, vec![json!(org_id)])).await?;
    for m in members {
        change_membership(w, org_id, &m.user_id, Some(m.role), None).await?;
    }
    table::delete(w.txn, "orgs", &json!(org_id)).await?;
    Ok(())
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub struct MemberLimits {
    pub max_members: usize,
    pub max_orgs: usize,
}

pub(super) async fn add_member(
    w: &mut Write<'_, '_>,
    org_id: &str,
    actor: &str,
    email: &str,
    role: Role,
    limits: MemberLimits,
) -> Result<MemberInfo> {
    let me = active_user(w.txn, actor).await?;
    let mine = require_role(w.txn, org_id, &me, Role::Admin).await?;
    if !mine.may_change(None, Some(role)) {
        return Err(Error::PermissionDenied("only owners can add owners".into()));
    }
    let user =
        super::user_by_email(w.txn, email).await?.ok_or_else(|| Error::not_found("no account has this email"))?;
    if get_member(w.txn, org_id, &user.id).await?.is_some() {
        return Err(Error::AlreadyExists("already a member".into()));
    }
    if count(w.txn, Count::Members, org_id).await? >= limits.max_members as i64 {
        let max = limits.max_members;
        return Err(Error::PermissionDenied(format!("an organization can have at most {max} members")));
    }
    org_slot(w.txn, &user.id, limits.max_orgs).await?;
    change_membership(w, org_id, &user.id, None, Some((role, w.now_ms))).await?;
    Ok(MemberInfo { user_id: user.id, email: user.email, role, added_at: w.now_ms })
}

pub(super) async fn set_role(
    w: &mut Write<'_, '_>,
    org_id: &str,
    actor: &str,
    user_id: &str,
    role: Role,
) -> Result<MemberInfo> {
    let me = active_user(w.txn, actor).await?;
    let mine = require_role(w.txn, org_id, &me, Role::Admin).await?;
    let m = get_member(w.txn, org_id, user_id).await?.ok_or_else(|| Error::not_found("not a member"))?;
    if !mine.may_change(Some(m.role), Some(role)) {
        return Err(Error::PermissionDenied("only owners can change owners".into()));
    }
    keep_an_owner(w.txn, org_id, m.role, Some(role)).await?;
    change_membership(w, org_id, user_id, Some(m.role), Some((role, m.added_at))).await?;
    MemberInfo::of(w.txn, Member { role, ..m }).await
}

pub(super) async fn remove_member(w: &mut Write<'_, '_>, org_id: &str, actor: &str, user_id: &str) -> Result<()> {
    let me = active_user(w.txn, actor).await?;
    let mine = require_role(w.txn, org_id, &me, Role::Reader).await?;
    let m = get_member(w.txn, org_id, user_id).await?.ok_or_else(|| Error::not_found("not a member"))?;
    if actor != user_id && !mine.may_change(Some(m.role), None) {
        return Err(Error::PermissionDenied(
            match mine.can_administer() {
                true => "only owners can remove owners",
                false => "this requires the admin role",
            }
            .into(),
        ));
    }
    keep_an_owner(w.txn, org_id, m.role, None).await?;
    change_membership(w, org_id, user_id, Some(m.role), None).await
}
