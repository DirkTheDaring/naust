use std::collections::HashMap;
use std::hash::{BuildHasher, Hash, Hasher};
use std::net::{IpAddr, Ipv4Addr};
use std::sync::Arc;
use std::sync::Mutex;

const IP_SHARDS: usize = 64;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum NormalizedClientKey {
    V4(Ipv4Addr),
    V6Prefix(u64), // Top 64 bits of IPv6 address
}

impl NormalizedClientKey {
    pub fn from_ip(ip: IpAddr) -> Self {
        match ip {
            IpAddr::V4(v4) => NormalizedClientKey::V4(v4),
            IpAddr::V6(v6) => {
                let seg = v6.segments();
                let prefix = ((seg[0] as u64) << 48)
                    | ((seg[1] as u64) << 32)
                    | ((seg[2] as u64) << 16)
                    | (seg[3] as u64);
                NormalizedClientKey::V6Prefix(prefix)
            }
        }
    }
}

pub struct IpConcurrencyLimiter {
    shards: Vec<Mutex<HashMap<NormalizedClientKey, usize>>>,
    max_per_ip: usize,
    trusted_bypass_cidrs: Vec<ipnet::IpNet>,
    build_hasher: std::collections::hash_map::RandomState,
}

impl IpConcurrencyLimiter {
    pub fn new(max_per_ip: usize, trusted_bypass_cidrs: Vec<ipnet::IpNet>) -> Self {
        let mut shards = Vec::with_capacity(IP_SHARDS);
        for _ in 0..IP_SHARDS {
            shards.push(Mutex::new(HashMap::new()));
        }
        Self {
            shards,
            max_per_ip,
            trusted_bypass_cidrs,
            build_hasher: std::collections::hash_map::RandomState::new(),
        }
    }

    pub fn is_bypassed(&self, ip: &IpAddr) -> bool {
        ip.is_loopback() || self.trusted_bypass_cidrs.iter().any(|net| net.contains(ip))
    }

    fn shard_idx(&self, key: &NormalizedClientKey) -> usize {
        let mut hasher = self.build_hasher.build_hasher();
        key.hash(&mut hasher);
        (hasher.finish() as usize) % IP_SHARDS
    }

    pub fn acquire(self: &Arc<Self>, ip: IpAddr) -> Result<Option<IpConnectionGuard>, ()> {
        if self.max_per_ip == 0 || self.is_bypassed(&ip) {
            return Ok(None);
        }
        let key = NormalizedClientKey::from_ip(ip);
        let shard_idx = self.shard_idx(&key);
        let shard = &self.shards[shard_idx];
        let mut map = shard.lock().unwrap_or_else(|e| e.into_inner());
        let count = map.entry(key).or_insert(0);
        if *count >= self.max_per_ip {
            Err(())
        } else {
            *count += 1;
            Ok(Some(IpConnectionGuard {
                limiter: self.clone(),
                key,
                shard_idx,
            }))
        }
    }

    pub fn release(&self, key: &NormalizedClientKey, shard_idx: usize) {
        let shard = &self.shards[shard_idx];
        let mut map = shard.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(count) = map.get_mut(key) {
            if *count <= 1 {
                map.remove(key);
            } else {
                *count -= 1;
            }
        }
    }
}

pub struct IpConnectionGuard {
    limiter: Arc<IpConcurrencyLimiter>,
    key: NormalizedClientKey,
    shard_idx: usize,
}

impl Drop for IpConnectionGuard {
    fn drop(&mut self) {
        self.limiter.release(&self.key, self.shard_idx);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    #[tokio::test]
    async fn test_ipv4_and_ipv6_64_normalization() {
        let ip1: IpAddr = "2001:db8:abcd:0012:0000:0000:0000:0001".parse().unwrap();
        let ip2: IpAddr = "2001:db8:abcd:0012:ffff:ffff:ffff:ffff".parse().unwrap();
        let ip3: IpAddr = "2001:db8:abcd:0013:0000:0000:0000:0001".parse().unwrap();

        assert_eq!(
            NormalizedClientKey::from_ip(ip1),
            NormalizedClientKey::from_ip(ip2),
            "IPv6 addresses in the same /64 must share normalized key"
        );
        assert_ne!(
            NormalizedClientKey::from_ip(ip1),
            NormalizedClientKey::from_ip(ip3),
            "IPv6 addresses in different /64 must have different normalized keys"
        );
    }

    #[test]
    fn test_ip_concurrency_limit_enforcement() {
        let limiter = Arc::new(IpConcurrencyLimiter::new(2, vec![]));
        let ip: IpAddr = "192.0.2.1".parse().unwrap();

        let g1 = limiter.acquire(ip).expect("permit 1");
        assert!(g1.is_some());
        let g2 = limiter.acquire(ip).expect("permit 2");
        assert!(g2.is_some());

        // 3rd request from same IP must be rejected
        let g3 = limiter.acquire(ip);
        assert!(g3.is_err(), "must reject exceeding concurrency limit");

        // Release one synchronously on drop
        drop(g1);

        let g4 = limiter.acquire(ip).expect("permit after release");
        assert!(g4.is_some());
    }

    #[test]
    fn test_trusted_bypass_cidr() {
        let bypass = vec![ipnet::IpNet::from_str("10.0.0.0/8").unwrap()];
        let limiter = Arc::new(IpConcurrencyLimiter::new(1, bypass));

        let ip: IpAddr = "10.1.2.3".parse().unwrap();
        let g1 = limiter.acquire(ip).expect("permit 1");
        assert!(g1.is_none(), "bypassed IP should return None (no limit)");
        let g2 = limiter.acquire(ip).expect("permit 2");
        assert!(g2.is_none(), "bypassed IP should return None (no limit)");
    }
}
