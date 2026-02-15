use std::collections::{HashMap, HashSet};

use crate::catalog::oid::Oid;
use crate::parser::ast::Expr;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RlsCommand {
    Select,
    Insert,
    Update,
    Delete,
    All,
}

impl RlsCommand {
    pub fn from_keyword(keyword: &str) -> Option<Self> {
        match keyword {
            "SELECT" => Some(Self::Select),
            "INSERT" => Some(Self::Insert),
            "UPDATE" => Some(Self::Update),
            "DELETE" => Some(Self::Delete),
            "ALL" => Some(Self::All),
            _ => None,
        }
    }

    pub fn matches(self, actual: Self) -> bool {
        self == Self::All || self == actual
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct RlsPolicy {
    pub name: String,
    pub relation_oid: Oid,
    pub command: RlsCommand,
    pub roles: Vec<String>,
    pub using_expr: Option<Expr>,
    pub check_expr: Option<Expr>,
}

#[derive(Debug, Clone, Default)]
pub struct RlsRegistry {
    enabled_relations: HashSet<Oid>,
    policies_by_relation: HashMap<Oid, Vec<RlsPolicy>>,
}

impl RlsRegistry {
    pub fn clear_relation(&mut self, relation_oid: Oid) {
        self.enabled_relations.remove(&relation_oid);
        self.policies_by_relation.remove(&relation_oid);
    }

    pub fn enable_for_relation(&mut self, relation_oid: Oid) {
        self.enabled_relations.insert(relation_oid);
    }

    pub fn disable_for_relation(&mut self, relation_oid: Oid) {
        self.enabled_relations.remove(&relation_oid);
    }

    pub fn is_enabled_for_relation(&self, relation_oid: Oid) -> bool {
        self.enabled_relations.contains(&relation_oid)
    }

    pub fn create_policy(&mut self, policy: RlsPolicy) -> Result<(), String> {
        let entry = self
            .policies_by_relation
            .entry(policy.relation_oid)
            .or_default();
        if entry
            .iter()
            .any(|existing| existing.name.eq_ignore_ascii_case(&policy.name))
        {
            return Err(format!("policy \"{}\" already exists", policy.name));
        }
        entry.push(policy);
        Ok(())
    }

    pub fn drop_policy(
        &mut self,
        relation_oid: Oid,
        policy_name: &str,
        if_exists: bool,
    ) -> Result<(), String> {
        let Some(policies) = self.policies_by_relation.get_mut(&relation_oid) else {
            if if_exists {
                return Ok(());
            }
            return Err(format!("policy \"{policy_name}\" does not exist"));
        };
        let Some(idx) = policies
            .iter()
            .position(|policy| policy.name.eq_ignore_ascii_case(policy_name))
        else {
            if if_exists {
                return Ok(());
            }
            return Err(format!("policy \"{policy_name}\" does not exist"));
        };
        policies.remove(idx);
        if policies.is_empty() {
            self.policies_by_relation.remove(&relation_oid);
        }
        Ok(())
    }

    pub fn applicable_policies(
        &self,
        relation_oid: Oid,
        command: RlsCommand,
        role_closure: &HashSet<String>,
    ) -> Vec<RlsPolicy> {
        self.policies_by_relation
            .get(&relation_oid)
            .map(|policies| {
                policies
                    .iter()
                    .filter(|policy| policy.command.matches(command))
                    .filter(|policy| {
                        policy
                            .roles
                            .iter()
                            .any(|role| role_closure.contains(&role.to_ascii_lowercase()))
                    })
                    .cloned()
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default()
    }
}
