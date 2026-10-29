//! Synchronous transitions; the caller serializes access to this whole state.

use crate::protocol::{AcquireReply, KvError, VersionedValue};
use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

struct Lease {
    acquisition_id: String,
    token: u64,
    deadline: Instant,
}

#[derive(Default)]
struct LockState {
    last_token: u64,
    current: Option<Lease>,
    // A late duplicate must never acquire again after the original lease ended.
    granted_ids: HashSet<String>,
}

pub struct KvState {
    values: HashMap<String, VersionedValue>,
    locks: HashMap<String, LockState>,
    lease_ttl: Duration,
    lease_ms: u64,
}

impl KvState {
    pub fn new(lease_ttl: Duration) -> Result<Self, KvError> {
        let lease_ms = u64::try_from(lease_ttl.as_millis()).map_err(|_| KvError::InvalidLease)?;
        if lease_ms == 0
            || Duration::from_millis(lease_ms) != lease_ttl
            || Instant::now().checked_add(lease_ttl).is_none()
        {
            return Err(KvError::InvalidLease);
        }
        Ok(Self {
            values: HashMap::new(),
            locks: HashMap::new(),
            lease_ttl,
            lease_ms,
        })
    }

    pub fn get(&self, key: &str) -> Result<VersionedValue, KvError> {
        self.values.get(key).cloned().ok_or(KvError::NoKey)
    }

    pub fn put(&mut self, key: &str, value: &str, version: u64) -> Result<u64, KvError> {
        match self.values.get_mut(key) {
            None if version == 0 => {
                self.values.insert(
                    key.to_owned(),
                    VersionedValue {
                        value: value.to_owned(),
                        version: 1,
                    },
                );
                Ok(1)
            }
            None => Err(KvError::NoKey),
            Some(entry) if entry.version != version => Err(KvError::Version),
            Some(entry) => {
                let next = entry
                    .version
                    .checked_add(1)
                    .ok_or(KvError::VersionExhausted)?;
                entry.value = value.to_owned();
                entry.version = next;
                Ok(next)
            }
        }
    }

    pub fn try_acquire_at(
        &mut self,
        lock: &str,
        acquisition_id: &str,
        now: Instant,
    ) -> Result<AcquireReply, KvError> {
        let state = self.locks.entry(lock.to_owned()).or_default();
        if state.granted_ids.contains(acquisition_id) {
            return match &state.current {
                Some(lease) if lease.acquisition_id == acquisition_id && now < lease.deadline => {
                    // A retried acquire cannot extend the original lease. Reporting the
                    // remaining time lets the caller schedule renewal sooner.
                    let remaining = lease.deadline.duration_since(now);
                    Ok(AcquireReply::Acquired {
                        token: lease.token,
                        lease_ms: millis_ceil(remaining),
                    })
                }
                _ => Err(KvError::LeaseLost),
            };
        }
        if let Some(lease) = &state.current {
            if now < lease.deadline {
                return Ok(AcquireReply::Busy {
                    retry_after_ms: millis_ceil(lease.deadline.duration_since(now)),
                });
            }
        }
        let token = state
            .last_token
            .checked_add(1)
            .ok_or(KvError::FenceExhausted)?;
        let deadline = now
            .checked_add(self.lease_ttl)
            .ok_or(KvError::InvalidLease)?;
        state.last_token = token;
        state.granted_ids.insert(acquisition_id.to_owned());
        state.current = Some(Lease {
            acquisition_id: acquisition_id.to_owned(),
            token,
            deadline,
        });
        Ok(AcquireReply::Acquired {
            token,
            lease_ms: self.lease_ms,
        })
    }

    pub fn renew_at(
        &mut self,
        lock: &str,
        acquisition_id: &str,
        token: u64,
        now: Instant,
    ) -> Result<(), KvError> {
        let state = self.locks.get_mut(lock).ok_or(KvError::LeaseLost)?;
        let lease = state.current.as_mut().ok_or(KvError::LeaseLost)?;
        if lease.acquisition_id != acquisition_id || lease.token != token || now >= lease.deadline {
            return Err(KvError::LeaseLost);
        }
        let deadline = now
            .checked_add(self.lease_ttl)
            .ok_or(KvError::InvalidLease)?;
        lease.deadline = deadline;
        Ok(())
    }

    pub fn release_at(
        &mut self,
        lock: &str,
        acquisition_id: &str,
        token: u64,
        now: Instant,
    ) -> Result<(), KvError> {
        let state = self.locks.get_mut(lock).ok_or(KvError::LeaseLost)?;
        let valid = state.current.as_ref().is_some_and(|lease| {
            lease.acquisition_id == acquisition_id && lease.token == token && now < lease.deadline
        });
        if !valid {
            return Err(KvError::LeaseLost);
        }
        state.current = None;
        Ok(())
    }

    pub fn put_fenced_at(
        &mut self,
        key: &str,
        value: &str,
        version: u64,
        lock: &str,
        token: u64,
        now: Instant,
    ) -> Result<u64, KvError> {
        let valid = self
            .locks
            .get(lock)
            .and_then(|state| state.current.as_ref())
            .is_some_and(|lease| lease.token == token && now < lease.deadline);
        if !valid {
            return Err(KvError::LeaseLost);
        }
        self.put(key, value, version)
    }
}

fn millis_ceil(duration: Duration) -> u64 {
    let millis = duration.as_nanos().div_ceil(1_000_000);
    u64::try_from(millis).unwrap_or(u64::MAX).max(1)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deadline_is_exclusive_and_stale_owner_cannot_touch_new_lease() {
        let ttl = Duration::from_millis(300);
        let mut state = KvState::new(ttl).unwrap();
        let start = Instant::now();
        assert_eq!(
            state.try_acquire_at("lock", "first", start),
            Ok(AcquireReply::Acquired {
                token: 1,
                lease_ms: 300
            })
        );
        assert_eq!(
            state.put_fenced_at("key", "one", 0, "lock", 1, start),
            Ok(1)
        );

        let end = start + ttl;
        assert!(matches!(
            state.try_acquire_at("lock", "second", end - Duration::from_nanos(1)),
            Ok(AcquireReply::Busy { .. })
        ));
        assert_eq!(
            state.renew_at("lock", "first", 1, end),
            Err(KvError::LeaseLost)
        );
        assert_eq!(
            state.put_fenced_at("key", "stale", 1, "lock", 1, end),
            Err(KvError::LeaseLost)
        );
        assert_eq!(
            state.try_acquire_at("lock", "first", end),
            Err(KvError::LeaseLost)
        );
        assert_eq!(
            state.try_acquire_at("lock", "second", end),
            Ok(AcquireReply::Acquired {
                token: 2,
                lease_ms: 300
            })
        );
        assert_eq!(
            state.release_at("lock", "first", 1, end),
            Err(KvError::LeaseLost)
        );
        assert_eq!(
            state.put_fenced_at("key", "stale", 1, "lock", 1, end),
            Err(KvError::LeaseLost)
        );
        assert_eq!(state.put_fenced_at("key", "two", 1, "lock", 2, end), Ok(2));
        assert_eq!(state.get("key").unwrap().value, "two");
    }

    #[test]
    fn duplicate_grant_does_not_extend_lease_but_renewal_does() {
        let ttl = Duration::from_millis(300);
        let mut state = KvState::new(ttl).unwrap();
        let start = Instant::now();
        state.try_acquire_at("lock", "first", start).unwrap();
        assert_eq!(
            state.try_acquire_at("lock", "first", start + Duration::from_millis(200)),
            Ok(AcquireReply::Acquired {
                token: 1,
                lease_ms: 100
            })
        );
        let renewed_at = start + Duration::from_millis(250);
        assert_eq!(state.renew_at("lock", "first", 1, renewed_at), Ok(()));
        assert!(matches!(
            state.try_acquire_at("lock", "other", start + ttl),
            Ok(AcquireReply::Busy { .. })
        ));
        assert_eq!(
            state.renew_at("lock", "first", 1, renewed_at + ttl),
            Err(KvError::LeaseLost)
        );
    }

    #[test]
    fn delayed_never_granted_attempt_can_temporarily_acquire_after_client_leaves() {
        let ttl = Duration::from_millis(300);
        let mut state = KvState::new(ttl).unwrap();
        let start = Instant::now();
        state.try_acquire_at("lock", "first", start).unwrap();
        assert!(matches!(
            state.try_acquire_at("lock", "abandoned", start + Duration::from_millis(1)),
            Ok(AcquireReply::Busy { .. })
        ));
        state
            .release_at("lock", "first", 1, start + Duration::from_millis(2))
            .unwrap();

        // No grant for this ID was recorded: the server cannot distinguish a
        // delayed request from one whose client is still waiting.
        let delayed = start + Duration::from_millis(3);
        assert!(matches!(
            state.try_acquire_at("lock", "abandoned", delayed),
            Ok(AcquireReply::Acquired { token: 2, .. })
        ));
        assert!(matches!(
            state.try_acquire_at("lock", "next", delayed),
            Ok(AcquireReply::Busy { .. })
        ));
        assert!(matches!(
            state.try_acquire_at("lock", "next", delayed + ttl),
            Ok(AcquireReply::Acquired { token: 3, .. })
        ));
    }

    #[test]
    fn counterexample_removing_tombstones_without_generation_check() {
        let mut state = KvState::new(Duration::from_millis(300)).unwrap();
        let now = Instant::now();
        state.try_acquire_at("lock", "old", now).unwrap();
        state.release_at("lock", "old", 1, now).unwrap();

        // Model the proposed change "remove history, keep fencing counter".
        // This is intentionally not the production state transition.
        state.locks.get_mut("lock").unwrap().granted_ids.clear();
        state.try_acquire_at("lock", "new", now).unwrap();
        state.release_at("lock", "new", 2, now).unwrap();
        assert!(matches!(
            state.try_acquire_at("lock", "old", now),
            Ok(AcquireReply::Acquired { token: 3, .. })
        ));
        // The stale request received a *new* token; comparing token numbers at
        // the resource cannot by itself identify the original request as old.
        assert_eq!(state.put_fenced_at("key", "old", 0, "lock", 3, now), Ok(1));
    }

    #[test]
    fn exhausted_counters_fail_without_changing_data_or_granting_a_lease() {
        let mut state = KvState::new(Duration::from_millis(10)).unwrap();
        state.values.insert(
            "key".into(),
            VersionedValue {
                value: "old".into(),
                version: u64::MAX,
            },
        );
        assert_eq!(
            state.put("key", "new", u64::MAX),
            Err(KvError::VersionExhausted)
        );
        assert_eq!(
            state.get("key"),
            Ok(VersionedValue {
                value: "old".into(),
                version: u64::MAX
            })
        );
        state.locks.insert(
            "lock".into(),
            LockState {
                last_token: u64::MAX,
                ..LockState::default()
            },
        );
        assert_eq!(
            state.try_acquire_at("lock", "owner", Instant::now()),
            Err(KvError::FenceExhausted)
        );
        let lock = state.locks.get("lock").unwrap();
        assert_eq!(lock.last_token, u64::MAX);
        assert!(lock.current.is_none());
        assert!(lock.granted_ids.is_empty());
    }

    #[test]
    fn many_puts_do_not_create_per_request_state() {
        let mut state = KvState::new(Duration::from_millis(100)).unwrap();
        for version in 0..50_000 {
            assert_eq!(state.put("key", "value", version), Ok(version + 1));
        }
        assert_eq!(state.values.len(), 1);
        assert!(state.locks.is_empty());
        assert_eq!(state.get("key").unwrap().version, 50_000);
    }
}
