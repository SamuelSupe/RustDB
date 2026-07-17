use super::{PredicateSidecarError, Result};

pub(super) fn bit_width(value: u64) -> u8 {
    if value == 0 {
        0
    } else {
        u8::try_from(u64::BITS - value.leading_zeros()).expect("u64 bit width fits in u8")
    }
}

pub(super) fn packed_len(value_count: usize, width: u8) -> Result<usize> {
    if width > 64 {
        return Err(PredicateSidecarError::Corrupt(format!(
            "invalid bit width {width}"
        )));
    }
    value_count
        .checked_mul(usize::from(width))
        .and_then(|bits| bits.checked_add(7))
        .map(|bits| bits / 8)
        .ok_or(PredicateSidecarError::TooLarge)
}

#[cfg(test)]
pub(super) fn pack(values: &[u64], width: u8) -> Result<Vec<u8>> {
    let mut output = Vec::with_capacity(packed_len(values.len(), width)?);
    let mut buffer = 0_u128;
    let mut buffered = 0_u8;

    for &value in values {
        if width < 64 && value >= (1_u64 << width) {
            return Err(PredicateSidecarError::Corrupt(format!(
                "value {value} does not fit in {width} bits"
            )));
        }
        buffer |= u128::from(value) << buffered;
        buffered = buffered
            .checked_add(width)
            .ok_or(PredicateSidecarError::TooLarge)?;
        while buffered >= 8 {
            output.push(buffer as u8);
            buffer >>= 8;
            buffered -= 8;
        }
    }
    if buffered != 0 {
        output.push(buffer as u8);
    }
    Ok(output)
}

#[cfg(test)]
pub(super) fn unpack(bytes: &[u8], value_count: usize, width: u8) -> Result<Vec<u64>> {
    Ok(PackedValues::new(bytes, value_count, width)?.collect())
}

pub(super) struct PackedValues<'a> {
    bytes: &'a [u8],
    remaining: usize,
    width: u8,
    byte_offset: usize,
    buffer: u128,
    buffered: u8,
    mask: u128,
}

impl<'a> PackedValues<'a> {
    pub(super) fn new(bytes: &'a [u8], value_count: usize, width: u8) -> Result<Self> {
        let expected = packed_len(value_count, width)?;
        if bytes.len() != expected {
            return Err(PredicateSidecarError::Corrupt(format!(
                "packed payload has {} bytes, expected {expected}",
                bytes.len()
            )));
        }
        let used_bits = value_count
            .checked_mul(usize::from(width))
            .ok_or(PredicateSidecarError::TooLarge)?
            % 8;
        if used_bits != 0 {
            let padding_mask = !((1_u8 << used_bits) - 1);
            if bytes.last().is_some_and(|byte| byte & padding_mask != 0) {
                return Err(PredicateSidecarError::Corrupt(
                    "non-zero bit-packed padding".to_owned(),
                ));
            }
        }
        let mask = match width {
            0 => 0,
            64 => u128::from(u64::MAX),
            _ => (1_u128 << width) - 1,
        };
        Ok(Self {
            bytes,
            remaining: value_count,
            width,
            byte_offset: 0,
            buffer: 0,
            buffered: 0,
            mask,
        })
    }
}

impl Iterator for PackedValues<'_> {
    type Item = u64;

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 {
            return None;
        }
        while self.buffered < self.width {
            let byte = self.bytes[self.byte_offset];
            self.buffer |= u128::from(byte) << self.buffered;
            self.byte_offset += 1;
            self.buffered += 8;
        }
        let value = (self.buffer & self.mask) as u64;
        self.buffer >>= self.width;
        self.buffered -= self.width;
        self.remaining -= 1;
        Some(value)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.remaining, Some(self.remaining))
    }
}

impl ExactSizeIterator for PackedValues<'_> {}
