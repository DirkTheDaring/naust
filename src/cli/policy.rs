#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum CommandPolicy {
    /// Pure local computation or configuration inspection; constructs no storage,
    /// acquires no locks, and has no side effects.
    Pure,

    /// Non-mutating inspection of existing persistent state.
    /// Absolutely no creation, rebuild, repair, readiness-marker write, quarantine, or deletion.
    ReadOnly,

    /// Non-mutating offline inspection that requires exclusive mutation authority
    /// to guarantee a coherent, point-in-time snapshot.
    ExclusiveInspection { lock_suffix: &'static str },

    /// Offline exclusive mutation requiring filesystem root lock (where applicable)
    /// followed by distributed mutation authority lease.
    ExclusiveMutation { lock_suffix: &'static str },

    /// Repository membership migration requiring exclusive authority and migration-specific
    /// readiness rules (can execute before readiness is achieved; writes marker upon completion).
    Migration { lock_suffix: &'static str },

    /// Administrative lock inspection and destructive recovery; must NOT attempt to acquire
    /// the deployment writer lock being inspected or cleared.
    BreakGlass,
}

impl CommandPolicy {
    pub fn requires_fs_root_lock(&self) -> bool {
        matches!(
            self,
            CommandPolicy::ExclusiveInspection { .. }
                | CommandPolicy::ExclusiveMutation { .. }
                | CommandPolicy::Migration { .. }
        )
    }

    pub fn requires_mutation_authority(&self) -> bool {
        matches!(
            self,
            CommandPolicy::ExclusiveInspection { .. }
                | CommandPolicy::ExclusiveMutation { .. }
                | CommandPolicy::Migration { .. }
        )
    }

    pub fn lock_suffix(&self) -> Option<&'static str> {
        match self {
            CommandPolicy::ExclusiveInspection { lock_suffix }
            | CommandPolicy::ExclusiveMutation { lock_suffix }
            | CommandPolicy::Migration { lock_suffix } => Some(lock_suffix),
            _ => None,
        }
    }
}
