
use std::{
    collections::HashMap,
    iter::FromIterator,
    mem,
    sync::Arc,
    time::{SystemTime, Duration}
};

use futures::{pin_mut, stream::{FuturesUnordered, StreamExt}};

use indexmap::IndexMap;

use tokio::sync::mpsc;

use tokio_postgres::{
    Client as PgClient,
    Error as PgError,
    Statement as PgStatement,
    binary_copy::BinaryCopyInWriter,
    types::Type as PgType,
};

use crate::{
    cache,
    labels::Labels,
    promb::types::{Sample, TimeSeries},
};

type NewSeries = IndexMap<Labels, Vec<usize>>;

pub struct Samples<'s>(i64, &'s [Sample]);

pub struct Client {
    pg_client: PgClient,
    get_id_for_labels: PgStatement,
    get_metrics_table: PgStatement,
}

impl Client {
    pub async fn from_pg_client(pg_client: PgClient) -> Result<Self, PgError> {
        let get_id_for_labels = pg_client.prepare("SELECT prom.get_series_id_for_key_value_array($1, $2, $3)")
            .await?;
        let get_metrics_table = pg_client.prepare("SELECT table_name FROM prom.get_or_create_metric_table_name($1)")
            .await?;
        Ok(Self{ pg_client, get_id_for_labels, get_metrics_table, })
    }

    pub async fn ingest(self: &Arc<Self>, time_series: &[TimeSeries]) -> Result<u64, Vec<PgError>> {
        let (new_series, mut samples) = parse_data(time_series).unwrap();

        self.insert_series(new_series, &mut samples).await?;

        let rows = self.insert_data(samples).await?;
        Ok(rows)
    }

    async fn insert_series(self: &Arc<Self>, series: NewSeries, samples: &mut HashMap<String, Vec<Samples<'_>>>)
    -> Result<(), Vec<PgError>> {
        // series.sort_keys();
        let mut inserts = FuturesUnordered::from_iter(series.into_iter()
            .map(|(l, i)| (self, l, i))
            .map(series_insert_query)
        );
        let mut errs = vec![];
        while let Some(res) = inserts.next().await {
            let (id, label, idxs) = match res {
                Ok(r) => r,
                Err(e) => {
                    errs.push(e);
                    continue // we still want o populate the cache with any new ids
                }
            };
            let samples = samples.get_mut(label.metric_name()).unwrap();
            for idx in idxs {
                samples[idx].0 = id
            }
            cache::set_id(label, id);
        }
        if !errs.is_empty() {
            return Err(errs)
        }
        Ok(())
    }

    async fn insert_data(self: &Arc<Self>, rows: HashMap<String, Vec<Samples<'_>>>)
    -> Result<u64, Vec<PgError>> {
        rows.iter()
            .map(|(m, _)| (self, m))
            .map(ensure_metric_names)
            .collect::<FuturesUnordered<_>>()
            .fold(Ok(()), |old, res|async {
                match (old, res) {
                    (old, Ok(..)) => old,
                    (Ok(..), Err(e)) => Err(vec![e]),
                    (Err(mut es), Err(e)) => {
                        es.push(e);
                        Err(es)
                    },
                }
            }).await?;
        rows.into_iter()
            .map(|(m, s)| (self, m, s))
            .map(insert_data_query)
            .collect::<FuturesUnordered<_>>()
            .fold(Ok(0), |rows, res| async {
                match (rows, res) {
                    (Ok(rows), Ok(count)) => Ok(rows + count),
                    (Err(es), Ok(..)) => Err(es),
                    (Ok(..), Err(e)) => Err(vec![e]),
                    (Err(mut es), Err(e)) => {
                        es.push(e);
                        Err(es)
                    },
                }
            })
            .await
    }

    async fn get_metric_table_name(self: &Arc<Self>, metric: String) -> Result<&'static str, PgError> {
        let row = self.pg_client.query_one(&self.get_metrics_table, &[&metric]).await?;
        let name: String = row.get(0);
        let table_name = cache::set_metric_table_name(metric, name);
        Ok(table_name)
    }
}

fn parse_data(time_series: &[TimeSeries])
-> Result<(NewSeries, HashMap<String, Vec<Samples>>), ()> {
    let mut new_series = IndexMap::new();
    let mut samples = HashMap::<String,_>::new();
    for series in time_series {
        let labels = Labels::from_proto_labels(series.get_labels());
        let (id, has_id) = cache::get_id(&labels)
            .map(|id| (id, true)).unwrap_or((-1, false));

        //FIXME handle lack of metric name
        let metric_samples = samples.get_mut(labels.metric_name());
        let metric_samples = match metric_samples {
            Some(ms) => ms,
            None => samples.entry(labels.metric_name().to_string())
                .or_insert_with(Vec::new),
        };
        let idx = metric_samples.len();
        metric_samples.push(Samples(id, series.get_samples()));

        if !has_id {
            new_series.entry(labels).or_insert_with(Vec::new).push(idx)
        }
    }
    Ok((new_series, samples))
}

async fn series_insert_query((client, label, idxs): (&Arc<Client>, Labels, Vec<usize>))
-> Result<(i64, Labels, Vec<usize>), PgError> {
    let (names, values, metric_name) = label.components();
    let row = client.pg_client.query_one(&client.get_id_for_labels, &[&metric_name, &names, &values])
        .await?;
    let id: i64 = row.get(0);
    Ok((id, label, idxs))
}

async fn ensure_metric_names((client, metric): (&Arc<Client>, &String)) -> Result<(), PgError> {
    if let None = cache::get_metric_table_name(&metric) {
        client.get_metric_table_name(metric.clone()).await?;
    }
    Ok(())
}

pub type InsertRequest<'a> = (Vec<Samples<'a>>, mpsc::Sender<Result<u64, PgError>>);

async fn insert_data_query((client, metric, samples): (&Arc<Client>, String, Vec<Samples<'_>>)) -> Result<u64, PgError> {
    let (s, mut result) = mpsc::channel(1);
    let mut inserter = cache::get_sender(metric, client);
    let samples = unsafe {
        launder_samples_lifetime(samples)
    };
    let r = inserter.send((samples, s)).await;
    if r.is_err() {
        panic!("failed to send")
    }
    result.recv().await.unwrap()
}

pub async fn insert_data_handler<'a>(metric: String, client: Arc<Client>, mut input: mpsc::Receiver<InsertRequest<'a>>) {
    let table_name = cache::get_metric_table_name(&metric).unwrap();
    let stmt = format!("COPY prom.{} FROM stdin BINARY", table_name);
    let mut pending_acks = vec![];
    loop {
        let (samples, mut ack_chan) = match input.recv().await {
            None => return,
            Some(req) => req,
        };
        let sink = client.pg_client.copy_in(&*stmt).await;
        let sink = match sink {
            Ok(sink) => sink,
            Err(e) => {
                let _ = ack_chan.send(Err(e)).await;
                continue
            },
        };
        pending_acks.push(ack_chan);
        let writer = BinaryCopyInWriter::new(sink, &[PgType::TIMESTAMPTZ, PgType::FLOAT8, PgType::INT4]);
        pin_mut!(writer);
        let mut res = Ok(0);
        for Samples(id, samples) in samples {
            for sample in samples {
                //TODO negative?
                let timestamp = SystemTime::UNIX_EPOCH + Duration::from_millis(sample.get_timestamp() as u64);
                let r = writer.as_mut().write(&[&timestamp, &sample.get_value(), &(id as i32)]).await;
                if let Err(e) = r {
                    res = Err(e);
                }
            }
        }
        'send: while pending_acks.len() < 10_000 {
            let (samples, ack_chan) = match input.try_recv() {
                Ok(s) => s,
                Err(..) => break 'send,
            };
            pending_acks.push(ack_chan);
            for Samples(id, samples) in samples {
                for sample in samples {
                    //TODO negative?
                    let timestamp = SystemTime::UNIX_EPOCH + Duration::from_millis(sample.get_timestamp() as u64);
                    let r = writer.as_mut().write(&[&timestamp, &sample.get_value(), &(id as i32)]).await;
                    if let Err(e) = r {
                        res = Err(e);
                        break 'send
                    }
                }
            }
        }
        if res.is_ok() {
            res = writer.finish().await;

        }
        let mut last = pending_acks.pop().unwrap();
        let _ = last.send(res).await;
        pending_acks.drain(..)
            .map(|mut c| async move { c.send(Ok(0)).await })
            .collect::<FuturesUnordered<_>>()
            .for_each(|_| futures::future::ready(()))
            .await
    }
}

unsafe fn launder_samples_lifetime<'a>(samples: Vec<Samples<'_>>) -> Vec<Samples<'a>> {
    mem::transmute(samples)
}
