
use std::{
    cmp::Ordering,
    hash::{Hash, Hasher},
    ptr::NonNull,
};

use crate::promb::types::Label;

// #[derive(PartialOrd, Ord)]
pub struct Labels {
    names: Box<[NonNull<str>]>,
    values: Box<[NonNull<str>]>,
    metric_name: Option<NonNull<str>>,
    flattened: Box<[u8]>,
}

impl Labels {
    pub fn from_proto_labels(labels: &[Label]) -> Self {
        let mut labels = &*labels;
        let mut owned_labels;
        // TODO is_sorted is unstable
        //if !labels.is_sorted() {
        let is_sorted = labels.windows(2).all(|w| w[0].name <= w[1].name);
        if is_sorted {
            owned_labels = labels.to_owned();
            owned_labels.sort_unstable_by(|a, b| a.name.partial_cmp(&b.name).unwrap());
            labels = &*owned_labels;
        }

        let mut flattened_len = labels.len() * 4;
        for label in labels {
            flattened_len += label.name.len() + label.value.len()
        }

        let mut flattened = Vec::with_capacity(flattened_len);
        let mut names = Vec::with_capacity(labels.len());
        let mut values = Vec::with_capacity(labels.len());
        let mut metric_name = None;

        unsafe {
            for label in labels {
                flattened.extend_from_slice(&(label.name.len() as u16).to_ne_bytes());
                let name_offset = flattened.len();
                flattened.extend_from_slice(label.name.as_bytes());

                flattened.extend_from_slice(&(label.value.len() as u16).to_ne_bytes());
                let value_offset = flattened.len();
                flattened.extend_from_slice(label.value.as_bytes());

                assert!(value_offset + label.value.len() <= flattened_len);

                let name = std::slice::from_raw_parts_mut(&mut flattened[name_offset], label.name.len())
                    as *mut [u8] as *mut str;
                let val = std::slice::from_raw_parts_mut(&mut flattened[value_offset], label.value.len())
                    as *mut [u8] as *mut str;
                names.push(NonNull::new_unchecked(name));
                values.push(NonNull::new_unchecked(val));
                if &*label.name == "__name__" {
                    metric_name = Some(NonNull::new_unchecked(val))
                }
            }
        }

        let names = names.into_boxed_slice();
        let values = values.into_boxed_slice();
        let flattened = flattened.into_boxed_slice();

        Self { names, values, metric_name, flattened }
    }

    pub fn metric_name(&self) -> &str {
        unsafe { self.metric_name.as_ref().map(|b| b.as_ref()).unwrap_or(&"") }
    }

    pub fn components(&self) -> (&[&str], &[&str], &str) {
        unsafe { (transmute_str_slice(&self.names), transmute_str_slice(&self.values), self.metric_name.as_ref().map(|b| b.as_ref()).unwrap_or(&"")) }
    }
}

unsafe fn transmute_str_slice<'a>(input: &'a [NonNull<str>]) -> &'a [&'a str] {
    std::mem::transmute(input)
}

unsafe impl Send for Labels {}
unsafe impl Sync for Labels {}

impl PartialEq<Labels> for Labels {
    fn eq(&self, other: &Self) -> bool {
        self.flattened == other.flattened
    }
}
impl Eq for Labels {}

impl Hash for Labels {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.flattened.hash(state);
    }
}
