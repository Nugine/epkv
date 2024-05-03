use roaring::RoaringTreemap;

#[derive(Default)]
pub struct OneMap {
    map: RoaringTreemap,
    bound: u64,
}

impl OneMap {
    #[inline]
    #[must_use]
    pub fn new(bound: u64) -> Self {
        Self { map: RoaringTreemap::new(), bound }
    }

    #[inline]
    #[must_use]
    pub const fn bound(&self) -> u64 {
        self.bound
    }

    #[inline]
    pub fn set(&mut self, pos: u64) {
        if pos > self.bound {
            self.map.insert(pos);
        }
    }

    #[inline]
    #[must_use]
    pub fn is_set(&self, pos: u64) -> bool {
        if pos > self.bound {
            return self.map.contains(pos);
        }
        true
    }

    #[inline]
    #[allow(clippy::arithmetic_side_effects)]
    pub fn update_bound(&mut self) {
        {
            let high = self.bound.saturating_add(2);
            let rank = self.bound + self.map.rank(high);
            if rank < high {
                let mid = self.bound.saturating_add(1);
                let rank = self.bound + self.map.rank(mid);
                if rank == mid {
                    self.map.remove(mid);
                    self.bound = mid;
                    return;
                }
            }
        }

        let mut low = self.bound.saturating_add(1);
        let mut high = self.bound.saturating_add(self.map.len()).saturating_add(1);
        while low < high {
            let mid = low + (high - low) / 2;
            let rank = self.bound + self.map.rank(mid);
            if rank >= mid {
                low = mid + 1;
            } else {
                high = mid;
            }
        }
        self.map.remove_range(self.bound.saturating_add(1)..low);
        self.bound = low - 1;
    }

    #[inline]
    pub fn set_bound(&mut self, bound: u64) {
        let (old, new) = (self.bound, bound);
        if old < new {
            self.map.remove_range(old.wrapping_add(1)..=new);
            self.bound = new;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::ops::Not;

    #[test]
    fn step() {
        let mut map = OneMap::new(0);

        for i in 0..100 {
            map.set(i);
            map.update_bound();
            assert_eq!(map.bound(), i);
        }
    }

    #[test]
    fn small() {
        let mut map = OneMap::new(9);

        map.set(10);

        map.set_bound(10);

        map.set(11);
        map.set(13);

        assert!(map.is_set(8));
        assert!(map.is_set(12).not());

        map.update_bound();
        assert_eq!(map.bound(), 11);

        map.set(12);
        map.update_bound();
        assert_eq!(map.bound(), 13);
    }

    #[test]
    fn large() {
        let mut map = OneMap::new(1_000);

        for i in 1001..=2000 {
            map.set(i);
        }
        map.update_bound();
        assert_eq!(map.bound(), 2000);
    }
}
