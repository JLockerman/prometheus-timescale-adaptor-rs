
use crate::promb::types::Label;

#[derive(PartialEq, Eq, Hash, Clone, PartialOrd, Ord)]
pub struct Labels {
    names: Box<[String]>,
    values: Box<[String]>,
    metric_name: Option<Box<str>>,
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
            //TODO sort by names only
            owned_labels.sort_unstable_by(|a, b| a.name.partial_cmp(&b.name).unwrap());
            labels = &*owned_labels;
        }

        let mut names = Vec::with_capacity(labels.len());
        let mut values = Vec::with_capacity(labels.len());
        let mut metric_name = None;

        for label in labels {
            names.push(label.name.to_string());
            values.push(label.value.to_string());
            if &*label.name == "__name__" {
                metric_name = Some(label.value.to_string().into_boxed_str())
            }
        }

        let names = names.into_boxed_slice();
        let values = values.into_boxed_slice();

        Self { names, values, metric_name}
    }

    pub fn metric_name(&self) -> &str {
        self.metric_name.as_ref().map(|b| &**b).unwrap_or(&"")
    }

    pub fn components(&self) -> (&[String], &[String], &str) {
        (&*self.names, &*self.values, self.metric_name.as_ref().map(|b| &**b).unwrap_or(&""))
    }
}
