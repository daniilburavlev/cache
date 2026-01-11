use bytes::{Buf, Bytes};
use core::fmt;
use std::{hash::Hash, io::Cursor};

use crate::error::CacheError;

const STRING_BYTE: u8 = b'+';
const ERROR_BYTE: u8 = b'-';
const BULK_BYTE: u8 = b'$';
const INTEGER_BYTE: u8 = b':';
const ARRAY_BYTE: u8 = b'*';

#[derive(Clone, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub enum Entity {
    Simple(String),
    Bulk(Bytes),
    Error(String),
    Integer(i64),
    Null,
    Array(Vec<Entity>),
}

impl Entity {
    pub fn array() -> Entity {
        Entity::Array(vec![])
    }

    pub fn push_bulk(&mut self, bytes: Bytes) {
        match self {
            Entity::Array(vec) => {
                vec.push(Entity::Bulk(bytes));
            }
            _ => panic!("not an array frame"),
        }
    }

    pub fn push(&mut self, frame: Self) {
        match self {
            Entity::Array(vec) => {
                vec.push(frame);
            }
            _ => panic!("not an array frame"),
        }
    }

    pub fn push_int(&mut self, value: i64) {
        match self {
            Entity::Array(vec) => {
                vec.push(Entity::Integer(value));
            }
            _ => panic!("not an array frame"),
        }
    }

    pub fn check(src: &mut Cursor<&[u8]>) -> Result<(), CacheError> {
        match get_u8(src)? {
            STRING_BYTE => {
                get_line(src)?;
                Ok(())
            }
            ERROR_BYTE => {
                get_line(src)?;
                Ok(())
            }
            INTEGER_BYTE => {
                let _ = get_decimal(src)?;
                Ok(())
            }
            BULK_BYTE => {
                if b'-' == peek_u8(src)? {
                    // $-1\r\n
                    skip(src, 4)
                } else {
                    let len: usize = get_decimal(src)?.try_into()?;
                    skip(src, len + 2)
                }
            }
            ARRAY_BYTE => {
                let len = get_decimal(src)?;

                for _ in 0..len {
                    Entity::check(src)?;
                }

                Ok(())
            }
            other => Err(format!("protocol error; invalid frame type byte `{}`", other).into()),
        }
    }

    pub fn parse(src: &mut Cursor<&[u8]>) -> Result<Entity, CacheError> {
        match get_u8(src)? {
            STRING_BYTE => {
                let line = get_line(src)?.to_vec();
                let string = String::from_utf8(line)?;
                Ok(Entity::Simple(string))
            }
            ERROR_BYTE => {
                let line = get_line(src)?.to_vec();
                let string = String::from_utf8(line)?;
                Ok(Entity::Error(string))
            }
            INTEGER_BYTE => {
                let num = get_decimal(src)?;
                Ok(Entity::Integer(num))
            }
            BULK_BYTE => {
                if ERROR_BYTE == peek_u8(src)? {
                    let line = get_line(src)?;
                    if line != b"-1" {
                        return Err("protocol error; invalid frame format".into());
                    }
                    Ok(Entity::Null)
                } else {
                    let len = get_decimal(src)?.try_into()?;
                    let n = len + 2;
                    if src.remaining() < n {
                        return Err(CacheError::Incomplete);
                    }
                    let data = Bytes::copy_from_slice(&src.chunk()[..len]);
                    skip(src, n)?;
                    Ok(Entity::Bulk(data))
                }
            }
            ARRAY_BYTE => {
                let len = get_decimal(src)?.try_into()?;
                let mut out = Vec::with_capacity(len);
                for _ in 0..len {
                    out.push(Entity::parse(src)?);
                }
                Ok(Entity::Array(out))
            }
            _ => unimplemented!(),
        }
    }
}

impl PartialEq<&str> for Entity {
    fn eq(&self, other: &&str) -> bool {
        match self {
            Entity::Simple(s) => s.eq(other),
            Entity::Bulk(s) => s.eq(other),
            _ => false,
        }
    }
}

impl fmt::Display for Entity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use std::str;

        match self {
            Entity::Simple(s) => s.fmt(f),
            Entity::Error(err) => write!(f, "error: {}", err),
            Entity::Integer(i) => i.fmt(f),
            Entity::Bulk(b) => match str::from_utf8(b) {
                Ok(s) => s.fmt(f),
                Err(_) => write!(f, "{:?}", b),
            },
            Entity::Null => "(nil)".fmt(f),
            Entity::Array(arr) => {
                for (i, part) in arr.iter().enumerate() {
                    if i > 0 {
                        write!(f, " ")?;
                    }
                    part.fmt(f)?;
                }
                Ok(())
            }
        }
    }
}

fn peek_u8(src: &mut Cursor<&[u8]>) -> Result<u8, CacheError> {
    if !src.has_remaining() {
        return Err(CacheError::Incomplete);
    }
    Ok(src.chunk()[0])
}

fn get_u8(src: &mut Cursor<&[u8]>) -> Result<u8, CacheError> {
    if !src.has_remaining() {
        return Err(CacheError::Incomplete);
    }
    Ok(src.get_u8())
}

fn skip(src: &mut Cursor<&[u8]>, n: usize) -> Result<(), CacheError> {
    if src.remaining() < n {
        return Err(CacheError::Incomplete);
    }
    src.advance(n);
    Ok(())
}

fn get_decimal(src: &mut Cursor<&[u8]>) -> Result<i64, CacheError> {
    use atoi::atoi;

    let line = get_line(src)?;

    atoi::<i64>(line).ok_or_else(|| "protocol error; invalid frame format".into())
}

fn get_line<'a>(src: &mut Cursor<&'a [u8]>) -> Result<&'a [u8], CacheError> {
    let start = src.position() as usize;
    let end = src.get_ref().len() - 1;

    for i in start..end {
        if src.get_ref()[i] == b'\r' && src.get_ref()[i + 1] == b'\n' {
            src.set_position((i + 2) as u64);
            return Ok(&src.get_ref()[start..i]);
        }
    }

    Err(CacheError::Incomplete)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn check_test() {
        check("+Hello\r\n");
        check("-ERROR\r\n");
        check(":1234\r\n");
        check("$-1\r\n");
        check("*1\r\n+Hello\r\n");
        check("$1\r\n1\r\n");
    }

    #[test]
    #[should_panic]
    fn check_test_failure() {
        check("+");
        check("-");
        check(":");
        check("$");
        check("*");
        check("$");
    }

    #[test]
    fn parse_str() {
        let buffer = "+Hello\r\n".as_bytes();
        let mut cursor = Cursor::new(buffer);
        match Entity::parse(&mut cursor).unwrap() {
            Entity::Simple(s) => assert_eq!(s, "Hello"),
            _ => panic!("invalid parsed type"),
        }
    }

    #[test]
    fn parse_num() {
        let buffer = ":1234\r\n".as_bytes();
        let mut cursor = Cursor::new(buffer);
        match Entity::parse(&mut cursor).unwrap() {
            Entity::Integer(s) => assert_eq!(s, 1234),
            _ => {}
        }
    }

    #[test]
    fn parse_bulk() {
        let buffer = "$1\r\n1\r\n".as_bytes();
        let mut cursor = Cursor::new(buffer);
        match Entity::parse(&mut cursor).unwrap() {
            Entity::Bulk(b) => {
                let mut iter = b.into_iter();
                assert_eq!(b'1', iter.next().unwrap());
            }
            _ => {}
        }
    }

    #[test]
    fn parse_arr() {
        let buffer = "*1\r\n+Hello\r\n".as_bytes();
        let mut cursor = Cursor::new(buffer);
        match Entity::parse(&mut cursor).unwrap() {
            Entity::Array(arr) => match arr.get(0).unwrap() {
                Entity::Simple(s) => assert_eq!(s, "Hello"),
                _ => {}
            },
            _ => {}
        }
    }

    #[test]
    fn parse_nil() {
        let buffer = "$-1\r\n".as_bytes();
        let mut cursor = Cursor::new(buffer);
        match Entity::parse(&mut cursor).unwrap() {
            Entity::Null => assert!(true),
            _ => panic!("invalid parsed type"),
        }
    }

    #[test]
    fn display_test() {
        let array = vec![
            Entity::Simple("Hello".to_string()),
            Entity::Integer(10),
            Entity::Null,
            Entity::Error("ERROR".to_string()),
        ];
        let _ = Entity::Array(array);
    }

    fn check(s: &str) {
        let buffer = s.as_bytes();
        let mut cursor = Cursor::new(buffer);
        Entity::check(&mut cursor).unwrap();
    }
}
