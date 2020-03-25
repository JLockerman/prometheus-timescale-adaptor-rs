
use std::{
    io::Read,
    sync::Arc,
};

use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Method, Request, Response, Server, StatusCode};

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

async fn write(client: Arc<Client>, req: Request<Body>) -> Result<Response<Body>, Box<dyn std::error::Error + Send + Sync>> {
    match (req.method(), req.uri().path()) {
        (&Method::POST, "/write") => {
            eprintln!("got write!");
            let body = hyper::body::to_bytes(req.into_body()).await?;
            eprintln!("got body! {}", body.len());
            let mut decompresser = snap::read::FrameDecoder::new(&*body);
            let mut buffer = vec![];
            decompresser.read_to_end(&mut buffer)?;
            eprintln!("decompressed!");
            let decompressed = buffer.into();
            let write_req: WriteRequest = protobuf::parse_from_carllerche_bytes(&decompressed)?;
            eprintln!("write_req!");
            match client.ingest(write_req.get_timeseries()).await {
                Ok(_rows) => {
                    eprintln!("response!");
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
            }

        }
        (method, path) => {
            eprintln!("unexpected req: {:?}, {:?}", method, path);
            let mut not_found = Response::default();
            *not_found.status_mut() = StatusCode::NOT_FOUND;
            Ok(not_found)
        }
    }
}
