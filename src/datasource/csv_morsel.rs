use bytes::{Bytes, BytesMut};

pub(super) struct RecordMorselizer {
    buffer: BytesMut,
    target_bytes: usize,
    quote: u8,
    escape: Option<u8>,
    skip_header: bool,
    in_quotes: bool,
    scan: usize,
}

impl RecordMorselizer {
    pub(super) fn new(
        target_bytes: usize,
        quote: u8,
        escape: Option<u8>,
        skip_header: bool,
    ) -> Self {
        Self {
            buffer: BytesMut::new(),
            target_bytes,
            quote,
            escape,
            skip_header,
            in_quotes: false,
            scan: 0,
        }
    }

    pub(super) fn push(&mut self, bytes: &[u8]) -> Vec<Bytes> {
        self.buffer.extend_from_slice(bytes);
        self.scan(false)
    }

    pub(super) fn finish(mut self) -> Vec<Bytes> {
        let mut morsels = self.scan(true);
        if self.skip_header {
            self.buffer.clear();
        } else if !self.buffer.is_empty() {
            morsels.push(self.buffer.split().freeze());
        }
        morsels
    }

    pub(super) fn buffered_len(&self) -> usize {
        self.buffer.len()
    }

    fn scan(&mut self, eof: bool) -> Vec<Bytes> {
        let mut output = Vec::new();
        while self.scan < self.buffer.len() {
            let byte = self.buffer[self.scan];
            if self.in_quotes && self.escape == Some(byte) {
                if self.scan + 1 == self.buffer.len() && !eof {
                    break;
                }
                self.scan = (self.scan + 2).min(self.buffer.len());
                continue;
            }
            if byte == self.quote {
                if self.in_quotes && self.buffer.get(self.scan + 1) == Some(&self.quote) {
                    self.scan += 2;
                    continue;
                }
                if self.scan + 1 == self.buffer.len() && !eof {
                    break;
                }
                self.in_quotes = !self.in_quotes;
            } else if byte == b'\n' && !self.in_quotes {
                self.scan += 1;
                if self.skip_header {
                    let _ = self.buffer.split_to(self.scan);
                    self.skip_header = false;
                    self.scan = 0;
                    continue;
                }
                if self.scan >= self.target_bytes {
                    output.push(self.buffer.split_to(self.scan).freeze());
                    self.scan = 0;
                    continue;
                }
                continue;
            }
            self.scan += 1;
        }
        output
    }
}

#[cfg(test)]
mod tests {
    use super::RecordMorselizer;

    #[test]
    fn splits_only_at_record_boundaries_and_drops_one_header() {
        let mut splitter = RecordMorselizer::new(8, b'"', None, true);
        let mut output = splitter.push(b"id,note\n1,\"first\n");
        output.extend(splitter.push(b"second\"\n2,\"a\"\"b\"\n3,c"));
        output.extend(splitter.finish());

        assert_eq!(output.len(), 3);
        assert_eq!(&output[0][..], b"1,\"first\nsecond\"\n");
        assert_eq!(&output[1][..], b"2,\"a\"\"b\"\n");
        assert_eq!(&output[2][..], b"3,c");
    }

    #[test]
    fn waits_when_quote_or_escape_is_split_across_chunks() {
        let mut splitter = RecordMorselizer::new(1, b'"', Some(b'\\'), false);
        assert!(splitter.push(b"1,\"a\\").is_empty());
        let mut output = splitter.push(b"\"b\"\n");
        output.extend(splitter.finish());
        assert_eq!(&output[0][..], b"1,\"a\\\"b\"\n");
    }
}
