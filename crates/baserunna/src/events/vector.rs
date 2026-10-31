//! Columnar value vectors used in parts and query execution.

use std::sync::Arc;

/// Dictionary code representing a null string.
pub const NULL_CODE: u32 = u32::MAX;

/// Dictionary-encoded string vector.
#[derive(Clone, Debug, PartialEq)]
pub struct Strs {
    pub dict: Arc<Vec<String>>,
    pub codes: Vec<u32>,
}

impl Strs {
    pub fn get(&self, i: usize) -> Option<&str> {
        match self.codes[i] {
            NULL_CODE => None,
            c => Some(self.dict[c as usize].as_str()),
        }
    }

    /// Encodes `vals` into a dictionary of distinct strings in first-seen
    /// order.
    pub fn from_values<'a>(vals: impl IntoIterator<Item = Option<&'a str>>) -> Strs {
        let mut dict: Vec<String> = Vec::new();
        let mut ids: crate::util::DetHashMap<&'a str, u32> = Default::default();
        let mut codes = Vec::new();
        for v in vals {
            codes.push(match v {
                None => NULL_CODE,
                Some(s) => *ids.entry(s).or_insert_with(|| {
                    dict.push(s.to_string());
                    (dict.len() - 1) as u32
                }),
            });
        }
        Strs { dict: Arc::new(dict), codes }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum Vector {
    Int(Vec<Option<i64>>),
    Float(Vec<Option<f64>>),
    Bool(Vec<Option<bool>>),
    Str(Strs),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Phys {
    Int = 0,
    Float = 1,
    Bool = 2,
    Str = 3,
}

impl Phys {
    pub fn from_tag(t: u8) -> Option<Phys> {
        [Phys::Int, Phys::Float, Phys::Bool, Phys::Str].get(t as usize).copied()
    }

    pub fn nulls(self, n: usize) -> Vector {
        match self {
            Phys::Int => Vector::Int(vec![None; n]),
            Phys::Float => Vector::Float(vec![None; n]),
            Phys::Bool => Vector::Bool(vec![None; n]),
            Phys::Str => Vector::Str(Strs { dict: Arc::new(Vec::new()), codes: vec![NULL_CODE; n] }),
        }
    }
}

impl Vector {
    pub fn phys(&self) -> Phys {
        match self {
            Vector::Int(_) => Phys::Int,
            Vector::Float(_) => Phys::Float,
            Vector::Bool(_) => Phys::Bool,
            Vector::Str(_) => Phys::Str,
        }
    }

    pub fn len(&self) -> usize {
        match self {
            Vector::Int(v) => v.len(),
            Vector::Float(v) => v.len(),
            Vector::Bool(v) => v.len(),
            Vector::Str(s) => s.codes.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn is_null(&self, i: usize) -> bool {
        match self {
            Vector::Int(v) => v[i].is_none(),
            Vector::Float(v) => v[i].is_none(),
            Vector::Bool(v) => v[i].is_none(),
            Vector::Str(s) => s.codes[i] == NULL_CODE,
        }
    }

    pub fn nulls(&self) -> usize {
        (0..self.len()).filter(|i| self.is_null(*i)).count()
    }

    /// Heap memory allocated for the value buffer, excluding any string
    /// dictionary.
    pub fn values_mem_size(&self) -> usize {
        use std::mem::size_of;
        match self {
            Vector::Int(v) => v.capacity() * size_of::<Option<i64>>(),
            Vector::Float(v) => v.capacity() * size_of::<Option<f64>>(),
            Vector::Bool(v) => v.capacity() * size_of::<Option<bool>>(),
            Vector::Str(s) => s.codes.capacity() * size_of::<u32>(),
        }
    }

    /// Total heap memory allocated for the vector, including any string
    /// dictionary.
    pub fn mem_size(&self) -> usize {
        self.values_mem_size()
            + match self {
                Vector::Str(s) => dict_mem_size(&s.dict),
                _ => 0,
            }
    }

    pub fn slice(&self, r: std::ops::Range<usize>) -> Vector {
        match self {
            Vector::Int(v) => Vector::Int(v[r].to_vec()),
            Vector::Float(v) => Vector::Float(v[r].to_vec()),
            Vector::Bool(v) => Vector::Bool(v[r].to_vec()),
            Vector::Str(s) => Vector::Str(Strs { dict: s.dict.clone(), codes: s.codes[r].to_vec() }),
        }
    }

    /// Concatenates `parts` into a vector of type `phys`, replacing mismatched
    /// vector types with nulls and merging distinct string dictionaries.
    pub fn concat(parts: Vec<Vector>, phys: Phys) -> Vector {
        if parts.len() == 1 {
            return parts.into_iter().next().unwrap_or_else(|| phys.nulls(0));
        }
        match phys {
            Phys::Int => Vector::Int(
                parts
                    .into_iter()
                    .flat_map(|p| match p {
                        Vector::Int(v) => v,
                        o => vec![None; o.len()],
                    })
                    .collect(),
            ),
            Phys::Float => Vector::Float(
                parts
                    .into_iter()
                    .flat_map(|p| match p {
                        Vector::Float(v) => v,
                        o => vec![None; o.len()],
                    })
                    .collect(),
            ),
            Phys::Bool => Vector::Bool(
                parts
                    .into_iter()
                    .flat_map(|p| match p {
                        Vector::Bool(v) => v,
                        o => vec![None; o.len()],
                    })
                    .collect(),
            ),
            Phys::Str => {
                let shared = match parts.first() {
                    Some(Vector::Str(f)) => {
                        parts.iter().all(|p| matches!(p, Vector::Str(s) if Arc::ptr_eq(&s.dict, &f.dict)))
                    }
                    _ => false,
                };
                if shared {
                    let dict = match &parts[0] {
                        Vector::Str(s) => s.dict.clone(),
                        _ => unreachable!("checked above"),
                    };
                    let codes = parts
                        .into_iter()
                        .flat_map(|p| match p {
                            Vector::Str(s) => s.codes,
                            o => vec![NULL_CODE; o.len()],
                        })
                        .collect();
                    return Vector::Str(Strs { dict, codes });
                }
                let mut vals: Vec<Option<String>> = Vec::new();
                for p in &parts {
                    match p {
                        Vector::Str(s) => vals.extend((0..s.codes.len()).map(|i| s.get(i).map(str::to_string))),
                        o => vals.extend(std::iter::repeat_n(None, o.len())),
                    }
                }
                Vector::Str(Strs::from_values(vals.iter().map(|v| v.as_deref())))
            }
        }
    }
}

/// Heap memory allocated for `dict` and its string buffers.
pub fn dict_mem_size(dict: &Arc<Vec<String>>) -> usize {
    16 + dict.capacity() * std::mem::size_of::<String>() + dict.iter().map(String::capacity).sum::<usize>()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn str_encoding_and_concat() {
        let a = Strs::from_values([Some("x"), None, Some("y"), Some("x")]);
        assert_eq!(a.dict.len(), 2);
        assert_eq!((a.get(0), a.get(1), a.get(3)), (Some("x"), None, Some("x")));
        let b = Strs::from_values([Some("z")]);
        let c = Vector::concat(vec![Vector::Str(a.clone()), Vector::Str(b)], Phys::Str);
        let Vector::Str(c) = c else { panic!() };
        let got: Vec<Option<&str>> = (0..5).map(|i| c.get(i)).collect();
        assert_eq!(got, vec![Some("x"), None, Some("y"), Some("x"), Some("z")]);
        let same = Vector::concat(vec![Vector::Str(a.clone()), Vector::Str(a.clone())], Phys::Str);
        let Vector::Str(s) = same else { panic!() };
        assert!(Arc::ptr_eq(&s.dict, &a.dict), "a shared dictionary is kept");
    }
}
