mod encode;

#[cfg(test)]
mod tests;

use arrow::record_batch::RecordBatch;
use sha2::{Digest, Sha256};

const MODULUS_BYTES: usize = 32;

pub(crate) struct MultisetChecksum {
    count: u64,
    sum: [u8; MODULUS_BYTES],
    xor: [u8; MODULUS_BYTES],
}

impl MultisetChecksum {
    pub(crate) fn new() -> Self {
        Self {
            count: 0,
            sum: [0; MODULUS_BYTES],
            xor: [0; MODULUS_BYTES],
        }
    }

    pub(crate) fn update_batch(&mut self, batch: &RecordBatch) -> Result<(), String> {
        for row in 0..batch.num_rows() {
            self.update_digest(encode::hash_row(batch, row)?);
        }
        Ok(())
    }

    fn update_digest(&mut self, digest: [u8; MODULUS_BYTES]) {
        let mut carry = 0_u16;
        for index in (0..MODULUS_BYTES).rev() {
            let value = u16::from(self.sum[index]) + u16::from(digest[index]) + carry;
            self.sum[index] = value as u8;
            carry = value >> 8;
            self.xor[index] ^= digest[index];
        }
        self.count = self.count.saturating_add(1);
    }

    pub(crate) fn finish(self) -> String {
        let mut digest = Sha256::new();
        digest.update(b"rustdb-multiset-sha256-v2");
        digest.update(self.count.to_le_bytes());
        digest.update(self.sum);
        digest.update(self.xor);
        format!("{:x}", digest.finalize())
    }
}
