use epkv_epaxos::id::InstanceId;

use bytemuck::{AnyBitPattern, NoUninit};

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

#[derive(Clone, Copy, NoUninit, AnyBitPattern)]
#[repr(C)]
pub struct InstanceFieldKey {
    prefix: u8,
    rid: Be64,
    lid: Be64,
    field: u8,
}

#[derive(Clone, Copy, NoUninit, AnyBitPattern)]
#[repr(C)]
pub struct GlobalFieldKey {
    prefix: u8,
    field: u8,
}

impl InstanceFieldKey {
    const PREFIX: u8 = 1;

    pub const FIELD_CMD: u8 = 1;
    pub const FIELD_PBAL: u8 = 2;
    pub const FIELD_STATUS: u8 = 3;
    pub const FIELD_OTHERS: u8 = 4;

    #[must_use]
    pub fn new(id: InstanceId, field: u8) -> Self {
        Self {
            prefix: Self::PREFIX,
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
}

impl GlobalFieldKey {
    const PREFIX: u8 = 2;

    pub const FIELD_ATTR_BOUNDS: u8 = 1;
    pub const FIELD_STATUS_BOUNDS: u8 = 2;

    #[must_use]
    pub fn new(field: u8) -> Self {
        Self { prefix: Self::PREFIX, field }
    }

    pub fn set_field(&mut self, field: u8) {
        self.field = field;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::fmt;

    #[allow(clippy::integer_arithmetic)]
    fn assert_unique<T: Eq + fmt::Debug>(arr: &[T]) {
        for i in 0..arr.len() {
            for j in (i + 1)..arr.len() {
                assert_ne!(arr[i], arr[j]);
            }
        }
    }

    #[test]
    fn prefix() {
        let prefixes = [InstanceFieldKey::PREFIX, GlobalFieldKey::PREFIX];

        assert!(prefixes.iter().copied().all(|p| p != 0));
        assert_unique(&prefixes);
    }

    #[test]
    fn fields() {
        {
            let fields = [
                InstanceFieldKey::FIELD_CMD,
                InstanceFieldKey::FIELD_PBAL,
                InstanceFieldKey::FIELD_STATUS,
                InstanceFieldKey::FIELD_OTHERS,
            ];
            assert!(fields.iter().copied().all(|p| p != 0));
            assert_unique(&fields);
        }
        {
            let fields = [
                GlobalFieldKey::FIELD_ATTR_BOUNDS,
                GlobalFieldKey::FIELD_STATUS_BOUNDS,
            ];
            assert!(fields.iter().copied().all(|p| p != 0));
            assert_unique(&fields);
        }
    }
}
