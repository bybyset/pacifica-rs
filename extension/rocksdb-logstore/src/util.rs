use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};

pub fn to_key(log_index: usize) -> Vec<u8> {
    let mut buf = Vec::with_capacity(8);
    buf.write_u64::<BigEndian>(log_index as u64).unwrap();
    buf
}

pub fn from_key(buf: &[u8]) -> usize {
    (&buf[0..8]).read_u64::<BigEndian>().unwrap() as usize
}
