//! Organization roles and database permissions. See SPEC §9.
//!
//! Each higher role includes all permissions of the roles below it:
//!
//! - `reader`: read data
//! - `writer`: write data and change schemas
//! - `admin`: create or delete databases and manage `reader`, `writer`, and `admin` members
//! - `owner`: manage all members and delete the organization

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

    /// Returns `true` if a member with role `self` can change a membership from `from` to `to`.
    ///
    /// Only owners can add, change, or remove owners.
    pub fn can_change_role(self, from: Option<Role>, to: Option<Role>) -> bool {
        let owners_involved = from == Some(Role::Owner) || to == Some(Role::Owner);
        self.can_administer() && (!owners_involved || self == Role::Owner)
    }
}

/// Effective database permissions for a caller.
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
