pub type Oid = u32;

pub const FIRST_NORMAL_OID: Oid = 16_384;

#[derive(Debug, Clone)]
pub struct OidGenerator {
    next: Oid,
}

impl Default for OidGenerator {
    fn default() -> Self {
        Self::new(FIRST_NORMAL_OID)
    }
}

impl OidGenerator {
    pub const fn new(start: Oid) -> Self {
        Self { next: start }
    }

    pub fn next_oid(&mut self) -> Oid {
        let oid = self.next;
        self.next = self
            .next
            .checked_add(1)
            .expect("catalog OID space exhausted");
        oid
    }
}
