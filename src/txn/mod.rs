use crate::tcop::engine::EngineStateSnapshot;

pub mod snapshot;
pub mod visibility;

use snapshot::TransactionSnapshots;
use visibility::VisibilityMode;

#[derive(Debug, Clone)]
struct SavepointFrame {
    name: String,
    snapshot: EngineStateSnapshot,
}

#[derive(Debug, Clone, Default)]
pub struct TransactionState {
    explicit_block: bool,
    failed_block: bool,
    snapshots: Option<TransactionSnapshots>,
    savepoints: Vec<SavepointFrame>,
}

impl TransactionState {
    pub fn begin(&mut self) {
        if self.explicit_block {
            return;
        }
        self.explicit_block = true;
        self.failed_block = false;
        self.snapshots = Some(TransactionSnapshots::capture());
        self.savepoints.clear();
    }

    pub fn commit(&mut self) -> Option<EngineStateSnapshot> {
        if !self.explicit_block {
            return None;
        }
        let committed = self
            .snapshots
            .as_ref()
            .map(|snapshots| snapshots.working().clone());
        self.clear();
        committed
    }

    pub fn rollback(&mut self) {
        self.clear();
    }

    pub fn savepoint(&mut self, name: String) -> Result<(), String> {
        if !self.explicit_block {
            return Err("SAVEPOINT can only be used in transaction blocks".to_string());
        }
        let Some(snapshots) = &self.snapshots else {
            return Err("transaction state missing working snapshot".to_string());
        };
        self.savepoints.push(SavepointFrame {
            name: name.to_ascii_lowercase(),
            snapshot: snapshots.working().clone(),
        });
        Ok(())
    }

    pub fn release_savepoint(&mut self, name: String) -> Result<(), String> {
        if !self.explicit_block {
            return Err("RELEASE SAVEPOINT can only be used in transaction blocks".to_string());
        }
        let normalized = name.to_ascii_lowercase();
        let Some(idx) = self
            .savepoints
            .iter()
            .rposition(|frame| frame.name == normalized)
        else {
            return Err(format!("savepoint \"{}\" does not exist", name));
        };
        self.savepoints.truncate(idx);
        Ok(())
    }

    pub fn rollback_to_savepoint(
        &mut self,
        name: String,
    ) -> Result<Option<EngineStateSnapshot>, String> {
        if !self.explicit_block {
            return Err("ROLLBACK TO SAVEPOINT can only be used in transaction blocks".to_string());
        }
        let normalized = name.to_ascii_lowercase();
        let Some(idx) = self
            .savepoints
            .iter()
            .rposition(|frame| frame.name == normalized)
        else {
            return Err(format!("savepoint \"{}\" does not exist", name));
        };
        let snapshot = self.savepoints[idx].snapshot.clone();
        if let Some(snapshots) = self.snapshots.as_mut() {
            snapshots.set_working(snapshot.clone());
        }
        self.savepoints.truncate(idx + 1);
        self.failed_block = false;
        Ok(Some(snapshot))
    }

    pub fn set_working_snapshot(&mut self, snapshot: EngineStateSnapshot) {
        if let Some(snapshots) = self.snapshots.as_mut() {
            snapshots.set_working(snapshot);
        }
    }

    pub fn working_snapshot(&self) -> Option<&EngineStateSnapshot> {
        self.snapshots.as_ref().map(|snapshots| snapshots.working())
    }

    pub fn base_snapshot(&self) -> Option<&EngineStateSnapshot> {
        self.snapshots.as_ref().map(|snapshots| snapshots.base())
    }

    pub fn mark_failed(&mut self) {
        if self.explicit_block {
            self.failed_block = true;
        }
    }

    pub fn clear_failed(&mut self) {
        self.failed_block = false;
    }

    pub fn in_explicit_block(&self) -> bool {
        self.explicit_block
    }

    pub fn is_aborted(&self) -> bool {
        self.explicit_block && self.failed_block
    }

    pub fn visibility_mode(&self) -> VisibilityMode {
        if self.explicit_block {
            VisibilityMode::TransactionLocal
        } else {
            VisibilityMode::Global
        }
    }

    fn clear(&mut self) {
        self.explicit_block = false;
        self.failed_block = false;
        self.snapshots = None;
        self.savepoints.clear();
    }
}
