use epkv_epaxos::id::InstanceId;
use epkv_utils::bits::{OneU8, TwoU8};

use std::fmt;

use bytemuck::{AnyBitPattern, CheckedBitPattern, NoUninit};

#[derive(Clone, Copy, NoUninit, AnyBitPattern)]
#[repr(transparent)]
struct Be64([u8; 8]);

impl Be64 {
    fn new(val: u64) -> Self {
        Self(val.to_be_bytes())
    }

    fn to_u64(self) -> u64 {
        u64::from_be_bytes(self.0)
    }
}

impl fmt::Debug for Be64 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("Be64").field(&self.to_u64()).finish()
    }
}

#[derive(Clone, Copy, NoUninit, CheckedBitPattern)]
#[repr(C)]
pub struct InstanceFieldKey {
    prefix: OneU8,
    rid: Be64,
    lid: Be64,
    field: u8,
}

#[derive(Clone, Copy, NoUninit, CheckedBitPattern)]
#[repr(C)]
pub struct GlobalFieldKey {
    prefix: TwoU8,
    field: u8,
}

impl InstanceFieldKey {
    pub const FIELD_STATUS: u8 = 1;
    pub const FIELD_PBAL: u8 = 2;
    pub const FIELD_CMD: u8 = 3;
    pub const FIELD_OTHERS: u8 = 4;

    #[must_use]
    pub fn new(id: InstanceId, field: u8) -> Self {
        Self {
            prefix: OneU8::VALUE,
            rid: Be64::new(id.0.raw_value()),
            lid: Be64::new(id.1.raw_value()),
            field,
        }
    }

    pub fn set_field(&mut self, field: u8) {
        self.field = field;
    }

    #[must_use]
    pub fn field(&self) -> u8 {
        self.field
    }

    #[must_use]
    pub fn id(&self) -> InstanceId {
        let rid = self.rid.to_u64().into();
        let lid = self.lid.to_u64().into();
        InstanceId(rid, lid)
    }

    pub fn set_id(&mut self, id: InstanceId) {
        self.rid = Be64::new(id.0.raw_value());
        self.lid = Be64::new(id.1.raw_value());
    }
}

impl GlobalFieldKey {
    pub const FIELD_ATTR_BOUNDS: u8 = 1;
    pub const FIELD_STATUS_BOUNDS: u8 = 2;

    #[must_use]
    pub fn new(field: u8) -> Self {
        Self { prefix: TwoU8::VALUE, field }
    }

    pub fn set_field(&mut self, field: u8) {
        self.field = field;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[allow(clippy::integer_arithmetic)]
    fn assert_nonzero_unique_sorted(arr: &[u8]) {
        assert!(arr.iter().copied().all(|p| p != 0));
        for i in 0..arr.len() {
            for j in (i + 1)..arr.len() {
                assert!(arr[i] < arr[j]);
            }
        }
    }

    #[test]
    fn fields() {
        {
            let fields = [
                InstanceFieldKey::FIELD_STATUS,
                InstanceFieldKey::FIELD_PBAL,
                InstanceFieldKey::FIELD_CMD,
                InstanceFieldKey::FIELD_OTHERS,
            ];
            assert_nonzero_unique_sorted(&fields);
        }
        {
            let fields = [
                GlobalFieldKey::FIELD_ATTR_BOUNDS,
                GlobalFieldKey::FIELD_STATUS_BOUNDS,
            ];
            assert_nonzero_unique_sorted(&fields);
        }
    }
}
