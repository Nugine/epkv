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
    #[allow(clippy::integer_arithmetic)]
    pub fn update_bound(&mut self) {
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
        self.map.remove_range(0..=low);
        self.bound = low - 1;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn simple() {
        let mut map = OneMap::new(9);
        map.set(10);
        map.set(11);
        map.set(13);

        map.update_bound();
        assert_eq!(map.bound(), 11);

        map.set(12);
        map.update_bound();
        assert_eq!(map.bound(), 13);
    }
}
