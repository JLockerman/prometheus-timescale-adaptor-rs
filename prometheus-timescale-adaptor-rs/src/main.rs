
use std::{
    cell::Cell,
    sync::Arc,
};

use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Method, Request, Response, Server, StatusCode};

use protobuf::{Clear, Message};

use timescale_pgmodel::{
    promb::remote::WriteRequest,
    ingestor::Client
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    use clap::{App, Arg};

    let matches = App::new("prometheus-timescale-adaptor")
        .arg(Arg::with_name("host")
            .short("h")
            .long("host")
            .value_name("hostname")
            .help("hostname of the timescale instance to run against")
            .takes_value(true)
            .required(true))
        .arg(Arg::with_name("port")
            .short("p")
            .long("port")
            .value_name("port")
            .help("port of the timescale instance to run against")
            .takes_value(true)
            .required(true))
        .arg(Arg::with_name("user")
            .short("u")
            .long("user")
            .value_name("user")
            .help("username to log into the database as")
            .takes_value(true)
            .required(true))
        .arg(Arg::with_name("database")
            .short("d")
            .long("database")
            .value_name("database")
            .help("database to run against")
            .takes_value(true)
            .required(true))
        .arg(Arg::with_name("password")
            .short("a")
            .long("password")
            .takes_value(true)
            .required(true))
        .get_matches();

    let host = matches.value_of("host").unwrap();
    let port = matches.value_of("port").unwrap();
    let user = matches.value_of("user").unwrap();
    let database = matches.value_of("database").unwrap();
    let password = matches.value_of("password").unwrap();

    let connection_str = format!("host={} port={} user={} dbname={} password='{}' connect_timeout=10",
        host, port, user, database, password);
    let (client, connection) =
        tokio_postgres::connect(&*connection_str, tokio_postgres::NoTls).await?;

    // The connection object performs the actual communication with the database,
    // so spawn it off to run on its own.
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    let client = Arc::new(Client::from_pg_client(client).await?);

    let service = make_service_fn(move |_| {
        let client = client.clone();
        async move {
            Ok::<_, hyper::Error>(service_fn(move |req| write(client.clone(), req)))
        }
    });

    let addr = ([127, 0, 0, 1], 9201).into();
    let server = Server::bind(&addr).serve(service);

    println!("Listening on http://{}", addr);

    server.await?;

    eprintln!("Exiting..");

    Ok(())
}

thread_local! {
    pub static WRITE_REQ_CACHE: Cell<Vec<WriteRequest>> = Cell::new(Vec::new());
    pub static WRITE_REQ_SIZE: Cell<usize> = Cell::new(0);
}

async fn write(client: Arc<Client>, req: Request<Body>) -> Result<Response<Body>, Box<dyn std::error::Error + Send + Sync>> {
    match (req.method(), req.uri().path()) {
        (&Method::POST, "/write") => {
            let body = hyper::body::to_bytes(req.into_body()).await?;
            let mut decompresser = snap::raw::Decoder::new();
            let buffer = decompresser.decompress_vec(&*body)?;
            let decompressed = buffer.into();
            let mut write_req = WRITE_REQ_CACHE.with(|c| {
                    let mut buf = c.take();
                    let req = buf.pop();
                    c.set(buf);
                    req
                }).unwrap_or_else(|| {
                    let mut req: WriteRequest = Default::default();
                    let expected_size = WRITE_REQ_SIZE.with(|s| s.get());
                    req.set_timeseries(Vec::with_capacity(expected_size).into());
                    req
                });
            {
                let mut decoder = protobuf::CodedInputStream::from_carllerche_bytes(&decompressed);
                parse_write_req(&mut write_req, &mut decoder)?;
            }

            let res = match client.ingest(write_req.get_timeseries()).await {
                Ok(_rows) => {
                    let mut ok = Response::default();
                    *ok.status_mut() = StatusCode::OK;
                    Ok(ok)
                }
                Err(errors) => {
                    eprintln!("pg errors: {:?}", errors);
                    let mut err = Response::default();
                    *err.status_mut() = StatusCode::BAD_REQUEST;
                    Ok(err)
                }
            };

            let size = write_req.get_timeseries().len();
            WRITE_REQ_SIZE.with(|s| {
                let old = s.get();
                if old > 0 {
                    s.set(1*size/3 + old*2/3);
                } else {
                    s.set(size)
                }
            });
            write_req.clear();
            WRITE_REQ_CACHE.with(|c| {
                let mut buf = c.take();
                buf.push(write_req);
                c.set(buf);
            });

            res
        }
        (method, path) => {
            eprintln!("unexpected req: {:?}, {:?}", method, path);
            let mut not_found = Response::default();
            *not_found.status_mut() = StatusCode::NOT_FOUND;
            Ok(not_found)
        }
    }
}

fn parse_write_req(write_req: &mut WriteRequest, is: &mut protobuf::CodedInputStream)
-> protobuf::ProtobufResult<()> {
    write_req.clear();
    write_req.merge_from(is)?;
    write_req.check_initialized()?;
    Ok(())
}
