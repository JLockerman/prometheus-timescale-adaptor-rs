
use std::{collections::HashMap, iter::FromIterator};

use futures::{pin_mut, stream::{FuturesUnordered, StreamExt}};

use indexmap::IndexMap;

use tokio_postgres::{
    Client as PgClient,
    Error as PgError,
    binary_copy::BinaryCopyInWriter,
    types::Type as PgType,
};

use crate::{
    cache,
    labels::Labels,
    promb::types::{Sample, TimeSeries},
};

pub type NewSeries = IndexMap<Labels, Vec<usize>>;

struct Samples<'s>(i64, &'s [Sample]);

pub async fn ingest(client: &PgClient, time_series: &[TimeSeries]) -> Result<u64, Vec<PgError>> {
    let (new_series, mut samples) = parse_data(time_series).unwrap();

    insert_series(client, new_series, &mut samples).await?;

    let rows = insert_data(client, samples).await?;
    Ok(rows)
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

async fn insert_series(client: &PgClient, mut series: NewSeries, samples: &mut HashMap<String, Vec<Samples<'_>>>)
-> Result<(), Vec<PgError>> {
    series.sort_keys();
    let mut inserts = FuturesUnordered::from_iter(series.into_iter()
        .map(|(l, i)| (client, l, i))
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

async fn series_insert_query((client, label, idxs): (&PgClient, Labels, Vec<usize>))
-> Result<(i64, Labels, Vec<usize>), PgError> {
    static GetSeriesIDForLabelSQL: &str = "SELECT get_series_id_for_key_value_array($1, $2, $3)";
    let (names, values, metric_name) = label.components();
    let row = client.query_one(GetSeriesIDForLabelSQL, &[&metric_name, &names, &values]).await?;
    let id: i64 = row.get(0);
    Ok((id, label, idxs))
}

async fn insert_data(client: &PgClient, rows: HashMap<String, Vec<Samples<'_>>>)
-> Result<u64, Vec<PgError>> {
    FuturesUnordered::from_iter(rows.into_iter()
            .map(|(m, s)| (client, m, s))
            .map(insert_data_query)
        ).fold(Ok(0), |rows, res| async {
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

async fn insert_data_query((client, metric, samples): (&PgClient, String, Vec<Samples<'_>>)) -> Result<u64, PgError> {
    let table_name = cache::get_metric_table_name(&metric);
    let table_name  = match table_name {
        Some(name) => name,
        None => get_metric_table_name(client, metric).await?,
    };
    let sink = client.copy_in(&*format!("COPY prom.{} FROM stdin BINARY", table_name)).await?;
    let writer = BinaryCopyInWriter::new(sink, &[PgType::TIMESTAMPTZ, PgType::FLOAT8, PgType::INT8]);
    pin_mut!(writer);
    for Samples(id, samples) in samples {
        for sample in samples {
            //TODO do we have to convert the timestamp
            writer.as_mut().write(&[&sample.get_timestamp(), &sample.get_value(), &id]).await?
        }
    }
    writer.finish().await
}

async fn get_metric_table_name(client: &PgClient, metric: String) -> Result<&'static str, PgError> {
    static GetCreateMetricsTableSQL: &str = "SELECT get_series_id_for_key_value_array($1, $2, $3)";
    let row = client.query_one(GetCreateMetricsTableSQL, &[&metric]).await?;
    let name: String = row.get(0);
    let table_name = cache::set_metric_table_name(metric, name);
    Ok(table_name)
}
