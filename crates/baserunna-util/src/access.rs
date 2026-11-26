//! Roles and effective permissions of a principal on a database. See SPEC §9.
//!
//! | role     | data (read) | data (write, schema) | create/delete databases | manage members        | delete org |
//! | -------- | ----------- | -------------------- | ----------------------- | --------------------- | ---------- |
//! | `reader` | yes         |                      |                         |                       |            |
//! | `writer` | yes         | yes                  |                         |                       |            |
//! | `admin`  | yes         | yes                  | yes                     | readers to admins     |            |
//! | `owner`  | yes         | yes                  | yes                     | everyone              | yes        |

use crate::error::{Error, Result};
use serde::{Deserialize, Serialize};

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
