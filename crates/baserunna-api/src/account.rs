//! Account, session, and organization types.

use crate::{Empty, Endpoint, NoBody, fill_route, is_false, json_body, with_query};
use http::{Method, StatusCode};
use serde::{Deserialize, Serialize};

/// `POST /v1/auth/signup`: registers an account and starts a session.
#[derive(Clone, Copy, Debug, Default)]
pub struct SignUp;

impl Endpoint for SignUp {
    const METHOD: Method = Method::POST;
    const ROUTE: &'static str = "/v1/auth/signup";
    const STATUS: StatusCode = StatusCode::CREATED;
    type Body = Credentials;
    type Response = Session;

    fn is_idempotent(&self, _: &Self::Body) -> bool {
        false
    }

    fn path(&self) -> String {
        Self::ROUTE.into()
    }
}

/// `POST /v1/auth/login`: starts a session with an email and password.
#[derive(Clone, Copy, Debug, Default)]
pub struct LogIn;

impl Endpoint for LogIn {
    const METHOD: Method = Method::POST;
    const ROUTE: &'static str = "/v1/auth/login";
    const STATUS: StatusCode = StatusCode::OK;
    type Body = Credentials;
    type Response = Session;

    fn is_idempotent(&self, _: &Self::Body) -> bool {
        false
    }

    fn path(&self) -> String {
        Self::ROUTE.into()
    }
}

/// `GET /v1/me`: reads the authenticated user and credential.
#[derive(Clone, Copy, Debug, Default)]
pub struct GetMe;

impl Endpoint for GetMe {
    const METHOD: Method = Method::GET;
    const ROUTE: &'static str = "/v1/me";
    const STATUS: StatusCode = StatusCode::OK;
    type Body = NoBody;
    type Response = Me;

    fn is_idempotent(&self, _: &Self::Body) -> bool {
        true
    }

    fn path(&self) -> String {
        Self::ROUTE.into()
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Credentials {
    pub email: String,
    pub password: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Session {
    pub token: String,
    pub user: User,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct User {
    pub id: String,
    pub email: String,
    pub email_verified: bool,
    pub is_admin: bool,
    pub disabled: bool,
    pub has_password: bool,
    pub google_linked: bool,
    /// Creation time in milliseconds since the Unix epoch.
    pub created_at: i64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Me {
    pub user: User,
    pub credential: Credential,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Credential {
    Session,
    ApiKey {
        id: String,
        /// Database that the key is restricted to, or `None` for all user databases.
        db: Option<String>,
        read_only: bool,
        /// Maximum requests per second per node, or `None` for the default limit.
        rate_limit: Option<u32>,
    },
}

/// `POST /v1/orgs`: creates an organization owned by the authenticated user.
#[derive(Clone, Copy, Debug, Default)]
pub struct CreateOrg;

impl Endpoint for CreateOrg {
    const METHOD: Method = Method::POST;
    const ROUTE: &'static str = "/v1/orgs";
    const STATUS: StatusCode = StatusCode::CREATED;
    type Body = NewOrg;
    type Response = Org;

    fn is_idempotent(&self, _: &Self::Body) -> bool {
        false
    }

    fn path(&self) -> String {
        Self::ROUTE.into()
    }
}

/// `POST /v1/orgs/{org}/members`: adds a registered user to an organization.
#[derive(Clone, Debug)]
pub struct AddMember {
    pub org: String,
}

impl Endpoint for AddMember {
    const METHOD: Method = Method::POST;
    const ROUTE: &'static str = "/v1/orgs/{org}/members";
    const STATUS: StatusCode = StatusCode::CREATED;
    type Body = NewMember;
    type Response = Member;

    fn is_idempotent(&self, _: &Self::Body) -> bool {
        false
    }

    fn path(&self) -> String {
        fill_route(Self::ROUTE, &[&self.org])
    }
}

/// `DELETE /v1/orgs/{org}/members/{user}`: removes a member from an organization.
#[derive(Clone, Debug)]
pub struct RemoveMember {
    pub org: String,
    pub user: String,
}

impl Endpoint for RemoveMember {
    const METHOD: Method = Method::DELETE;
    const ROUTE: &'static str = "/v1/orgs/{org}/members/{user}";
    const STATUS: StatusCode = StatusCode::OK;
    type Body = NoBody;
    type Response = Empty;

    fn is_idempotent(&self, _: &Self::Body) -> bool {
        false
    }

    fn path(&self) -> String {
        fill_route(Self::ROUTE, &[&self.org, &self.user])
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NewOrg {
    pub name: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Org {
    pub id: String,
    pub name: String,
    pub created_at: i64,
    pub role: Role,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Role {
    Reader,
    Writer,
    Admin,
    Owner,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NewMember {
    pub email: String,
    pub role: Role,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Member {
    pub user_id: String,
    pub email: String,
    pub role: Role,
    pub added_at: i64,
}

json_body!(Credentials, Session, Me, NewOrg, Org, NewMember, Member);

/// `POST /v1/api-keys`: creates an API key for the session's user. Only a session can manage keys.
#[derive(Clone, Copy, Debug, Default)]
pub struct CreateApiKey;

impl Endpoint for CreateApiKey {
    const METHOD: Method = Method::POST;
    const ROUTE: &'static str = "/v1/api-keys";
    const STATUS: StatusCode = StatusCode::CREATED;
    type Body = NewApiKey;
    type Response = CreatedApiKey;

    fn is_idempotent(&self, _: &Self::Body) -> bool {
        false
    }

    fn path(&self) -> String {
        Self::ROUTE.into()
    }
}

/// `GET /v1/api-keys`: lists the session user's API keys.
#[derive(Clone, Debug, Default)]
pub struct ListApiKeys {
    pub query: PageQuery,
}

impl Endpoint for ListApiKeys {
    const METHOD: Method = Method::GET;
    const ROUTE: &'static str = "/v1/api-keys";
    const STATUS: StatusCode = StatusCode::OK;
    type Body = NoBody;
    type Response = ApiKeyPage;

    fn is_idempotent(&self, _: &Self::Body) -> bool {
        true
    }

    fn path(&self) -> String {
        with_query(Self::ROUTE.into(), &self.query)
    }
}

/// `DELETE /v1/api-keys/{id}`: revokes an API key.
#[derive(Clone, Debug)]
pub struct DeleteApiKey {
    pub id: String,
}

impl Endpoint for DeleteApiKey {
    const METHOD: Method = Method::DELETE;
    const ROUTE: &'static str = "/v1/api-keys/{id}";
    const STATUS: StatusCode = StatusCode::OK;
    type Body = NoBody;
    type Response = Empty;

    fn is_idempotent(&self, _: &Self::Body) -> bool {
        false
    }

    fn path(&self) -> String {
        fill_route(Self::ROUTE, &[&self.id])
    }
}

/// Page size and position of a listing.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PageQuery {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NewApiKey {
    pub name: String,
    /// Database to restrict the key to, or `None` for all of the user's databases.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub db: Option<String>,
    #[serde(default, skip_serializing_if = "is_false")]
    pub read_only: bool,
    /// Maximum requests per second per node, or `None` for the default limit.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rate_limit: Option<u32>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreatedApiKey {
    /// Secret to authenticate with, returned only once.
    pub key: String,
    pub api_key: ApiKey,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApiKey {
    pub id: String,
    pub name: String,
    pub db: Option<String>,
    pub read_only: bool,
    pub rate_limit: Option<u32>,
    pub created_at: i64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApiKeyPage {
    pub api_keys: Vec<ApiKey>,
    pub next_cursor: Option<String>,
}

json_body!(NewApiKey, CreatedApiKey, ApiKeyPage);
