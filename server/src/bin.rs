use oxigraph::SledStore;
use oxigraph::io::DatasetFormat;
use oxigraph::io::GraphFormat;
use oxigraph::model;
use oxigraph::sparql;
use actix_files::NamedFile;
use actix_web::{
    web,
    HttpRequest,
    HttpResponse,
    http,
    error,
};
use serde_derive::Deserialize;
use derive_more::{Display, Error};
use std::path::PathBuf;
use std::io;

struct AppState {
    store: SledStore,
}

#[actix_web::main]
async fn main() -> io::Result<()> {
    use actix_web::{App, HttpServer};
    env_logger::init();
    println!("Starting server on 127.0.0.1:8080 ...");
    let app_state = web::Data::new(AppState {
        store: SledStore::open("example.db")?
    });

    HttpServer::new(move || {
        App::new()
            .configure(config_app(app_state.clone()))
    })
    .bind(("127.0.0.1", 8080))?
    .run()
    .await

}

fn config_app(app_state: web::Data<AppState>) -> Box<dyn Fn(&mut web::ServiceConfig)> {
    Box::new(move |cfg: &mut web::ServiceConfig| {
        cfg.app_data(app_state.clone())
            .service(
                web::resource("/")
                    .route(web::get().to(get_index))
            )
            .service(
                web::resource("/query")
                    .route(web::get().to(get_query))
                    .route(web::post().to(post_query))
            )
            // .service(get_query)
            .service(
                web::resource("/update")
                    .route(web::post().to(post_update))
            )
            .service(
                web::resource("/{path:store.*}")
                    .route(web::put().to(put_store))
                    .route(web::head().to(head_store))
                    .route(web::get().to(get_store))
                    .route(web::post().to(post_store))
                    .route(web::delete().to(delete_store))
            );
    })
}

// #[get("/")]
async fn get_index() -> Result<NamedFile, AppError> {
    let path = PathBuf::from("templates/index.html");
    Ok(NamedFile::open(path)?)
}

// #[get("/query")]
async fn get_query(request: HttpRequest, state: web::Data<AppState>) -> Result<HttpResponse, AppError> {
//     log::info!("query: {:?}", form);
//     format!("get_query: {:?}", form)
    configure_and_evaluate_sparql_query(state, &url_query(&request), None, request)
}

async fn post_query(request: HttpRequest, state: web::Data<AppState>, payload: web::Payload) -> Result<HttpResponse, AppError> {
    use http::header;
    use actix_web::FromRequest;
    use actix_web::dev;

    let mut payload: dev::Payload = payload.into_inner();

    // let mut bytes = web::BytesMut::new();
    // while let Some(item) = payload.next().await {
    //     bytes.extend_from_slice(&item?);
    // }

    if let Some(content_type) = request.headers().get(header::CONTENT_TYPE) {
        let content_type = content_type.to_str()?;
        if content_type == "application/sparql-query" {
            let body: String = String::from_request(&request, &mut payload).await?;
            configure_and_evaluate_sparql_query(state, &url_query(&request), Some(body), request)
        } else if content_type == "application/x-www-form-urlencoded" {
            let buffer = web::Bytes::from_request(&request, &mut payload).await?;
            configure_and_evaluate_sparql_query(state, &buffer, None, request)
        } else {
            Ok(HttpResponse::UnsupportedMediaType()
               .body(format!("Not supported Content-Type given: {}", content_type)))
        }
    } else {
        Ok(HttpResponse::BadRequest()
            .body("No Content-Type given"))
    }
}

async fn delete_store(request: HttpRequest, info: web::Query<StoreGraphInfo>, state: web::Data<AppState>) -> Result<HttpResponse, AppError> {
    use model::{GraphName, GraphNameRef};

    if let Some(target) = store_target(&request, info.into_inner())? {
        match target {
            GraphName::DefaultGraph => state.store.clear_graph(GraphNameRef::DefaultGraph)?,
            GraphName::NamedNode(target) => {
                if state.store.contains_named_graph(&target)? {
                    state.store.remove_named_graph(&target)?;
                } else {
                    return Ok(HttpResponse::NotFound()
                       .body(format!("The graph {} does not exists", target)));
                }
            }
            GraphName::BlankNode(target) => {
                if state.store.contains_named_graph(&target)? {
                    state.store.remove_named_graph(&target)?;
                } else {
                    return Ok(HttpResponse::NotFound()
                       .body(format!("The graph {} does not exists", target)));
                }
            }
        } 
    } else {
        state.store.clear()?;
    }
    Ok(HttpResponse::NoContent().finish())
}

async fn get_store(request: HttpRequest, info: web::Query<StoreGraphInfo>, state: web::Data<AppState>) -> Result<HttpResponse, AppError> {
    use model::GraphName;

    let mut body = Vec::default();
    let format = if let Some(target) = store_target(&request, info.into_inner())? {
        if !match &target {
            GraphName::DefaultGraph => true,
            GraphName::NamedNode(target) => state.store.contains_named_graph(target)?,
            GraphName::BlankNode(target) => state.store.contains_named_graph(target)?,
        } {
            return Ok(HttpResponse::NotFound()
                .body(format!("The graph {} does not exists", target)));
        }
        let format = graph_content_negotiation(request)?;
        state.store.dump_graph(&mut body, format, &target)?;
        format.media_type()
    } else {
        let format = dataset_content_negotiation(request)?;
        state.store.dump_dataset(&mut body, format)?;
        format.media_type()
    };
    Ok(HttpResponse::Ok()
        .header(http::header::CONTENT_TYPE, format)
        .body(body))
}

async fn head_store(request: HttpRequest, info: web::Query<StoreGraphInfo>, state: web::Data<AppState>) -> Result<HttpResponse, AppError> {
    use model::GraphName;

    if let Some(target) = store_target(&request, info.into_inner())? {
        if match &target {
            GraphName::DefaultGraph => true,
            GraphName::NamedNode(target) => state.store.contains_named_graph(target)?,
            GraphName::BlankNode(target) => state.store.contains_named_graph(target)?,
        } {
            Ok(HttpResponse::Ok().finish())
        } else {
            Ok(HttpResponse::NotFound()
                .body(format!("The graph {} does not exists", target)))
        }
    } else {
        Ok(HttpResponse::Ok().finish())
    }
}

// #[post("/store")]
async fn post_store(req: HttpRequest, state: web::Data<AppState>, body: String, info: web::Query<StoreGraphInfo>) -> Result<HttpResponse, AppError> {
    use model::{GraphName, NamedNode};
    use mime::Mime;
    use std::str::FromStr;

    if let Some(content_type) = req.headers().get("content-type") {
        let content_type: Mime = Mime::from_str(content_type.to_str()?)?;
        println!("content-type: {:?}", content_type);
        if let Some(target) = store_target(&req, info.into_inner())? {
            if let Some(format) = GraphFormat::from_media_type(content_type.essence_str()) {
                let new = !match &target {
                    GraphName::NamedNode(target) => state.store.contains_named_graph(target)?,
                    GraphName::BlankNode(target) => state.store.contains_named_graph(target)?,
                    GraphName::DefaultGraph => true,
                };
                state.store
                    .load_graph(
                        io::BufReader::new(io::Cursor::new(body)),
                        format,
                        &target,
                        None,
                    )?;
                Ok(if new {
                    HttpResponse::Created().finish()
                } else {
                    HttpResponse::NoContent().finish()
                })
            } else {
                Ok(HttpResponse::UnsupportedMediaType()
                   .body(format!("No supported Content-Type given: {}", content_type)))
            }
        } else if let Some(format) = DatasetFormat::from_media_type(content_type.essence_str()) {
            state.store
                .load_dataset(
                    io::BufReader::new(io::Cursor::new(body)),
                    format,
                    None
                ).map_err(AppError::BadInput)?;
            return Ok(HttpResponse::NoContent().finish());
        } else if let Some(format) = GraphFormat::from_media_type(content_type.essence_str()) {
            println!("url: {}", req.uri());
            let graph = NamedNode::new(
                base_url(&req, Some(&format!("/store/{:x}", rand::random::<u128>())))?.to_string()
            )?;

            state.store
                .load_graph(
                    io::BufReader::new(io::Cursor::new(body)),
                    format,
                    &graph,
                    None
            )?;
            Ok(HttpResponse::Created()
                .header(http::header::LOCATION, graph.into_string())
                .finish())
        } else {
            println!("no supported media type");
            Ok(HttpResponse::UnsupportedMediaType().body(format!("No supported Content-Type given: {}", content_type)))
        }
    } else {
        println!("no content type");
        Ok(HttpResponse::BadRequest().body("No Content-Type given."))
    }
}

async fn put_store(request: HttpRequest, info: web::Query<StoreGraphInfo>, payload: web::Bytes, state: web::Data<AppState>) -> Result<HttpResponse, AppError> {
    use http::header;
    use mime::Mime;
    use std::str::FromStr;
    use model::GraphName;

    println!("put_store: content_type = {:#?}", request.headers().get(header::CONTENT_TYPE));
    if let Some(content_type) = request.headers().get(header::CONTENT_TYPE) {
        let content_type: Mime = Mime::from_str(content_type.to_str()?)?;
        println!("put_store: query = {:?}", info);
        if let Some(target) = store_target(&request, info.into_inner())? {
            if let Some(format) = GraphFormat::from_media_type(content_type.essence_str()) {
                let new = !match &target {
                    GraphName::NamedNode(target) => {
                        if state.store.contains_named_graph(target)? {
                            state.store.clear_graph(target)?;
                            true
                        } else {
                            state.store.insert_named_graph(target)?;
                            false
                        }
                    },
                    GraphName::BlankNode(target) => {
                        if state.store.contains_named_graph(target)? {
                            state.store.clear_graph(target)?;
                            true
                        } else {
                            state.store.insert_named_graph(target)?;
                            false
                        }
                    },
                    GraphName::DefaultGraph => {
                        state.store.clear_graph(&target)?;
                        true
                    }
                };
                state.store
                    .load_graph(
                        io::BufReader::new(io::Cursor::new(payload)),
                        format,
                        &target,
                        None
                    )?;
                if new {
                    Ok(HttpResponse::Created().finish())
                } else {
                    Ok(HttpResponse::NoContent().finish())
                }
            } else {
                Ok(HttpResponse::UnsupportedMediaType()
                    .body(format!("No supported Content-Type given: {}", content_type)))
            }
        } else if let Some(format) = DatasetFormat::from_media_type(content_type.essence_str()) {
            todo!("got a format")
        } else {
            Ok(HttpResponse::UnsupportedMediaType()
                .body(format!("No supported Content-Type given: {}", content_type)))
        }
    } else {
        Ok(HttpResponse::BadRequest()
            .body("No Content-Type given"))
    }
}

async fn post_update(request: HttpRequest, payload: web::Payload, state: web::Data<AppState>) -> Result<HttpResponse, AppError> {
    use http::header;
    use mime::Mime;
    use std::str::FromStr;
    use actix_web::FromRequest;

    if let Some(content_type) = request.headers().get(header::CONTENT_TYPE) {
        let content_type: Mime = Mime::from_str(content_type.to_str()?)?;
        let mut payload = payload.into_inner();
        if content_type.essence_str() == "application/sparql-update" {
            let buffer = String::from_request(&request, &mut payload).await?;
                        
            configure_and_evaluate_sparql_update(
                state,
                &url_query(&request),
                Some(buffer),
                request,
            )
        } else if content_type.essence_str() == "application/x-www-form-urlencoded" {
            let buffer = web::Bytes::from_request(&request, &mut payload).await?;
            configure_and_evaluate_sparql_update(
                state,
                &buffer,
                None,
                request
            )
        } else {
            Ok(HttpResponse::UnsupportedMediaType()
               .body(format!("Not supported Content-Type given: {}", content_type)))
        }
    } else {
        Ok(HttpResponse::BadRequest()
           .body("No Content-Type given"))
    }
}


#[derive(Deserialize, Debug)]
struct StoreGraphInfo {
    default: Option<String>,
    graph: Option<String>,
}

#[derive(Deserialize, Debug)]
#[serde(deny_unknown_fields)]
struct QueryInfo {
    query: String,
    #[serde(rename = "default-graph-uri")]
    default_graph_uri: Option<Vec<String>>,
    #[serde(rename = "named-graph-uri")]
    named_graph_uri: Option<Vec<String>>,
}

fn url_query(request: &HttpRequest) -> Vec<u8> {
    request.uri().query().unwrap_or("").as_bytes().to_vec()
}

fn configure_and_evaluate_sparql_query(
    state: web::Data<AppState>,
    encoded: &[u8], // Vec<u8>,
    mut query: Option<String>,
    request: HttpRequest,
) -> Result<HttpResponse, AppError> {
    let mut default_graph_uris = Vec::new();
    let mut named_graph_uris = Vec::new();
    for (k, v) in form_urlencoded::parse(&encoded) {
        match k.as_ref() {
            "query" => {
                if query.is_some() {
                    return Err(AppError::BadRequest(InnerError::Str("Multiple query parameters provided")));
                }
                query = Some(v.into_owned())
            }
            "default-graph-uri" => default_graph_uris.push(v.into_owned()),
            "named-graph-uri" => named_graph_uris.push(v.into_owned()),
            _ => {
                return Err(AppError::BadRequestString(format!("Unexpected parameter: {}", k)));
            },
        }
    }
    if let Some(query) = query {
        evaluate_sparql_query(state, query, default_graph_uris, named_graph_uris, request)
    } else {
        Err(AppError::BadRequest(InnerError::Str("You should set the 'query' parameter")))
    }
}

fn evaluate_sparql_query(
    state: web::Data<AppState>,
    query: String,
    default_graph_uris: Vec<String>,
    named_graph_uris: Vec<String>,
    request: HttpRequest,
) -> Result<HttpResponse, AppError> {
    use sparql::{Query, QueryResults, QueryResultsFormat};

    let mut query = Query::parse(&query, Some(&base_url(&request, None)?.to_string()))?;
    let default_graph_uris = default_graph_uris
        .into_iter()
        .map(|e| Ok(model::NamedNode::new(e)?.into()))
        .collect::<Result<Vec<model::GraphName>, model::IriParseError>>()
        .map_err(AppError::UrlParseError)?;
    let named_graph_uris = named_graph_uris
        .into_iter()
        .map(|e| Ok(model::NamedNode::new(e)?.into()))
        .collect::<Result<Vec<model::NamedOrBlankNode>, model::IriParseError>>()
        .map_err(AppError::UrlParseError)?;

    if !default_graph_uris.is_empty() || !named_graph_uris.is_empty() {
        query.dataset_mut().set_default_graph(default_graph_uris);
        query
            .dataset_mut()
            .set_available_named_graphs(named_graph_uris);
    }

    let results = state.store.query(query)?;
    //TODO: stream
    if let QueryResults::Graph(_) = results {
        let format = graph_content_negotiation(request)?;
        let mut body = Vec::default();
        results.write_graph(&mut body, format)?;
        Ok(HttpResponse::Ok()
            .content_type(format.media_type())
            .body(body))
    } else {
        let format = content_negotiation(
            request,
            &[
                QueryResultsFormat::Xml.media_type(),
                QueryResultsFormat::Json.media_type(),
                QueryResultsFormat::Csv.media_type(),
                QueryResultsFormat::Tsv.media_type(),
            ],
            QueryResultsFormat::from_media_type,
        )?;
        let mut body = Vec::default();
        results.write(&mut body, format)?;
        Ok(HttpResponse::Ok()
            .header(http::header::CONTENT_TYPE, format.media_type())
            .body(body))
    }
}

fn configure_and_evaluate_sparql_update(
    state: web::Data<AppState>,
    encoded: &[u8],
    mut update: Option<String>,
    request: HttpRequest,
) -> Result<HttpResponse, AppError> {
    let mut default_graph_uris = Vec::new();
    let mut named_graph_uris = Vec::new();
    for (k, v) in form_urlencoded::parse(&encoded) {
        match k.as_ref() {
            "update" => {
                if update.is_some() {
                    return Ok(HttpResponse::BadRequest()
                       .body("Multiple update parameters provided"));
                }
                update = Some(v.into_owned())
            }
            "using-graph-uri" => default_graph_uris.push(v.into_owned()),
            "using-named-graph-uri" => named_graph_uris.push(v.into_owned()),
            _ => {
                return Ok(HttpResponse::BadRequest()
                    .body(format!("Unexpected parameter: {}", k)));
            }
        }
    }
    if let Some(update) = update {
        evaluate_sparql_update(state, update, default_graph_uris, named_graph_uris, request)
    } else {
        Ok(HttpResponse::BadRequest()
           .body("You should set the 'update' parameter"))
    }
}

fn evaluate_sparql_update(
    state: web::Data<AppState>,
    update: String,
    default_graph_uris: Vec<String>,
    named_graph_uris: Vec<String>,
    request: HttpRequest,
    ) -> Result<HttpResponse, AppError> {
    use sparql::{
        algebra::{GraphUpdateOperation},
        Update,
    };
    use model::{GraphName, NamedNode, NamedOrBlankNode};

    let mut update =
        Update::parse(&update, Some(&base_url(&request, None)?.to_string()))?;
    let default_graph_uris = default_graph_uris
        .into_iter()
        .map(|e| Ok(NamedNode::new(e)?.into()))
        .collect::<Result<Vec<GraphName>, AppError>>()?;
    let named_graph_uris = named_graph_uris
        .into_iter()
        .map(|e| Ok(NamedNode::new(e)?.into()))
        .collect::<Result<Vec<NamedOrBlankNode>, AppError>>()?;
    if !default_graph_uris.is_empty() || !named_graph_uris.is_empty() {
        for operation in &mut update.operations {
            if let GraphUpdateOperation::DeleteInsert { using, .. } = operation {
                if !using.is_default_dataset() {
                    return Ok(HttpResponse::BadRequest()
                       .body(
                        "using-graph-uri and using-named-graph-uri must not be used with a SPARQL UPDATE containing USING"));
                }
                using.set_default_graph(default_graph_uris.clone());
                using.set_available_named_graphs(named_graph_uris.clone());
            }
        }
    }
    state.store.update(update)?;
    Ok(HttpResponse::NoContent().finish())
}

fn store_target(request: &HttpRequest, info: StoreGraphInfo) -> Result<Option<model::GraphName>, AppError> {
    use oxigraph::model::NamedNode;

    if request.uri().path() == "/store" {
        if let Some(graph) = info.graph {
            if info.default.is_some() {
                Err(AppError::BadRequest(InnerError::Str("Both graph and default parameters should not be set at the same time")))
            } else {
                Ok(Some(NamedNode::new(
                    base_url(request, Some(&graph))?
                        .to_string()
                )?.into()))
            }
        } else if info.default.is_some() {
            Ok(Some(model::GraphName::DefaultGraph))
        } else {
            Ok(None)
        }
    } else {
        Ok(Some(NamedNode::new(
            base_url(request, None)?.to_string()
                              )?.into()))
    }
}

fn graph_content_negotiation(request: HttpRequest) -> Result<GraphFormat, AppError> {
    content_negotiation(
        request,
        &[
            GraphFormat::NTriples.media_type(),
            GraphFormat::Turtle.media_type(),
            GraphFormat::RdfXml.media_type(),
        ],
        GraphFormat::from_media_type,
    )
}

fn dataset_content_negotiation(request: HttpRequest) -> Result<DatasetFormat, AppError> {
    content_negotiation(
        request,
        &[
            DatasetFormat::NQuads.media_type(),
            DatasetFormat::TriG.media_type(),
        ],
        DatasetFormat::from_media_type,
    )
}

fn content_negotiation<F>(
    request: HttpRequest,
    supported: &[&str],
    parse: impl Fn(&str) -> Option<F>,
) -> Result<F, AppError> {
    use http::header;
    use std::str::FromStr;
    use mime::Mime;

    let accept_vec: Vec<header::QualityItem<Mime>> = request
        .headers()
        .get_all(header::ACCEPT)
        .map(|h: &header::HeaderValue| {
            let h_str = h.to_str().unwrap();
            let q_item: header::QualityItem<Mime> = header::QualityItem::from_str(h_str).unwrap();
            q_item
        })
        .collect();
    if accept_vec.is_empty() {
        parse(supported.first().ok_or_else(|| {
            AppError::InternalServerError(
                "No default MIME type provided"
            )
        })?)

    } else {
        let accept = header::Accept(accept_vec);
        let supported: Vec<Mime> = supported
            .iter()
            .map(|h| Mime::from_str(h).unwrap())
            .collect();
        parse(negotiate(accept, &supported)?.essence_str())
    }
    .ok_or_else(|| AppError::InternalServerError( "Unknown mime type"))
}

fn negotiate(accept: http::header::Accept, supported: &Vec<mime::Mime>) -> Result<mime::Mime, AppError> {
    for accepted in accept.mime_precedence() {
        if supported.contains(&accepted) {
            return Ok(accepted);
        }
    }
    Err(AppError::InternalServerError("error"))
}

#[derive(Debug, Display, Error)]
enum AppError {
    #[display(fmt = "io error")]
    IoError(io::Error),
    #[display(fmt = "bad input: {}", _0)]
    BadInput(io::Error),
    #[display(fmt = "bad url: {}", _0)]
    BadUrl(actix_web::client::HttpError),
    #[display(fmt = "url parse error: {}", _0)]
    UrlParseError(model::IriParseError),
    #[display(fmt = "bad request: {}", _0)]
    BadRequest(InnerError),
    #[display(fmt = "bad request: {}", _0)]
    BadRequestString(#[error(not(source))] String),
    #[display(fmt = "internal server error")]
    InternalServerError(#[error(not(source))] &'static str),
    #[display(fmt = "query parse error: {}", _0)]
    QueryParseError(sparql::ParseError),
    #[display(fmt = "query evalution error: {}", _0)]
    QueryEvaluationError(sparql::EvaluationError),
    #[display(fmt = "parse error: {}", _0)]
    ParseError(InnerError),
    #[display(fmt = "tostr error: {}", _0)]
    ToStrError(http::header::ToStrError),
    #[display(fmt = "bad request: {}", _0)]
    BadPayload(actix_web::Error),
}

impl error::ResponseError for AppError {
    fn error_response(&self) -> HttpResponse {
        use actix_web::dev::HttpResponseBuilder;
        println!("error: {:#?}", self);
        HttpResponseBuilder::new(self.status_code())
            .set_header(http::header::CONTENT_TYPE, "text/html; charset=utf-8")
            .body(self.to_string())
    }

    fn status_code(&self) -> http::StatusCode {
        match *self {
            AppError::BadInput(_) => http::StatusCode::BAD_REQUEST,
            AppError::BadUrl(_) => http::StatusCode::BAD_REQUEST,
            AppError::BadRequest(_) => http::StatusCode::BAD_REQUEST,
            AppError::BadRequestString(_) => http::StatusCode::BAD_REQUEST,
            AppError::BadPayload(_) => http::StatusCode::BAD_REQUEST,
            AppError::QueryParseError(_) => http::StatusCode::BAD_REQUEST,
            AppError::QueryEvaluationError(_) => http::StatusCode::BAD_REQUEST,
            _ => http::StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

impl From<io::Error> for AppError {
    fn from(err: io::Error) -> AppError {
        use io::ErrorKind;

        match err.kind() {
            ErrorKind::UnexpectedEof => AppError::BadRequest(InnerError::IoError(err)),
            _ => AppError::IoError(err),
        }
    }
}

impl From<model::IriParseError> for AppError {
    fn from(err: model::IriParseError) -> AppError {
        AppError::UrlParseError(err)
    }
}

impl From<sparql::ParseError> for AppError {
    fn from(err: sparql::ParseError) -> AppError {
        AppError::QueryParseError(err)
    }
}

impl From<sparql::EvaluationError> for AppError {
    fn from(err: sparql::EvaluationError) -> AppError {
        AppError::QueryEvaluationError(err)
    }
}

impl From<http::header::ToStrError> for AppError {
    fn from(err: http::header::ToStrError) -> AppError {
        AppError::ToStrError(err)
    }
}

impl From<actix_web::Error> for AppError {
    fn from(err: actix_web::Error) -> AppError {
        AppError::BadPayload(err)
    }
}

impl From<mime::FromStrError> for AppError {
    fn from(err: mime::FromStrError) -> AppError {
        AppError::ParseError(InnerError::MimeFromStr(err))
    }
}

#[derive(Debug, Display, Error)]
enum InnerError {
    #[display(fmt = "{}", _0)]
    Str(#[error(not(source))] &'static str),
    #[display(fmt = "{}", _0)]
    String(#[error(not(source))] String),
    #[display(fmt = "{}", _0)]
    MimeFromStr(mime::FromStrError),
    #[display(fmt = "{}", _0)]
    IoError(io::Error),
    #[display(fmt = "{}", _0)]
    ParseError(#[error(not(source))] error::ParseError),
}

fn base_url(request: &HttpRequest, path: Option<&str>) -> Result<http::Uri, AppError> {
    let mut uri = http::Uri::builder();
    if let Some(scheme) = request.uri().scheme() {
        uri = uri.scheme(scheme.as_str());
    }
    if let Some(host) = request.uri().host() {
        uri = uri.authority(host)
    }
    if let Some(path) = path {
        uri = uri.path_and_query(path);
    } else {
        uri = uri.path_and_query(request.uri().path());
    }
    Ok(uri.build().map_err(AppError::BadUrl)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use actix_web::{http, test, App};
    use tempfile::{tempdir};

    mod utils {
        use super::*;

        #[test]
        fn absolute_uri() {
            let req = test::TestRequest::with_uri("http://example.com/eat?my=shorts")
                .to_http_request();
            assert_eq!(base_url(&req, None).unwrap(), http::Uri::from_static("http://example.com/eat"));
        }

        #[test]
        fn absolute_uri_replace_path() {
            let req = test::TestRequest::with_uri("http://example.com/eat?my=shorts")
                .to_http_request();
            assert_eq!(base_url(&req, Some("/store/foo")).unwrap(), http::Uri::from_static("http://example.com/store/foo"));
        }
    }

    #[actix_rt::test]
    async fn get_ui() {
        let mut app = test::init_service(
            App::new()
                .service(
                    web::resource("/")
                        .route(web::get().to(get_index))
                )
        ).await;
        let req = test::TestRequest::with_header("content-type", "text/plain").to_request();
        let resp = test::call_service(&mut app, req).await;
        assert!(resp.status().is_success());
    }

    mod store {
        use super::*;

        #[actix_rt::test]
        async fn post_dataset_file() {
            let path = tempdir().unwrap();
            let app_state = web::Data::new(
                AppState {
                    store: SledStore::open(path.path()).unwrap()
                }
            );
            let mut app = test::init_service(
                App::new()
                    .configure(
                        config_app(app_state.clone()))
            ).await;
            let req = test::TestRequest::post()
                .uri("/store")
                .header("Content-Type", "application/trig")
                .set_payload("<http://example.com> <http://example.com> <http://example.com> .")
                .to_request();
            let resp = test::call_service(&mut app, req).await;
            assert_eq!(resp.status(), http::StatusCode::NO_CONTENT);
        }

        #[actix_rt::test]
        async fn post_graph_file() {
            let path = tempdir().unwrap();
            let app_state = web::Data::new(
                AppState {
                    store: SledStore::open(path.path()).unwrap()
                }
            );
            let mut app = test::init_service(
                App::new()
                    .configure(
                        config_app(app_state.clone()))
            ).await;
            let req = test::TestRequest::post()
                .uri("http://example.com/store")
                .header("Content-Type", "text/turtle")
                .set_payload("<http://example.com/ns/data#i01> <http://example.com/ns/book#firstName> \"Richard\" .")
                .to_request();
            let resp = test::call_service(&mut app, req).await;
            assert_eq!(resp.status(), http::StatusCode::CREATED);
            assert!(resp.headers().get(http::header::LOCATION).is_some());
        }

        #[actix_rt::test]
        async fn post_graph_file_default() {
            let path = tempdir().unwrap();
            let app_state = web::Data::new(
                AppState {
                    store: SledStore::open(path.path()).unwrap()
                }
            );
            let mut app = test::init_service(
                App::new()
                    .configure(
                        config_app(app_state.clone()))
            ).await;
            let req = test::TestRequest::post()
                .uri("http://example.com/store?default")
                .header("Content-Type", "text/turtle")
                .set_payload("<http://example.com/ns/data#i01> <http://example.com/ns/book#firstName> \"Richard\" .")
                .to_request();
            let resp = test::call_service(&mut app, req).await;
            assert_eq!(resp.status(), http::StatusCode::NO_CONTENT);
            // assert!(resp.headers().get(http::header::LOCATION).is_some());
        }

        #[actix_rt::test]
        async fn post_no_content() {
            let path = tempdir().unwrap();
            let app_state = web::Data::new(
                AppState {
                    store: SledStore::open(path.path()).unwrap()
                }
            );
            let mut app = test::init_service(
                App::new()
                    .configure(
                        config_app(app_state.clone()))
            ).await;
            let req = test::TestRequest::post()
                .uri("/store")
                .to_request();
            let resp = test::call_service(&mut app, req).await;
            assert_eq!(resp.status(), http::StatusCode::BAD_REQUEST);
        }

        #[actix_rt::test]
        async fn post_unsupported_file() {
            let path = tempdir().unwrap();
            let app_state = web::Data::new(
                AppState {
                    store: SledStore::open(path.path()).unwrap()
                }
            );
            let mut app = test::init_service(
                App::new()
                    .configure(
                        config_app(app_state.clone()))
            ).await;
            let req = test::TestRequest::post()
                .header("Content-Type", "text/foo")
                .uri("/store")
                .to_request();
            let resp = test::call_service(&mut app, req).await;
            assert_eq!(resp.status(), http::StatusCode::UNSUPPORTED_MEDIA_TYPE);
        }

        #[actix_rt::test]
        async fn post_wrong_file() {
            let path = tempdir().unwrap();
            let app_state = web::Data::new(
                AppState {
                    store: SledStore::open(path.path()).unwrap()
                }
            );
            let mut app = test::init_service(
                App::new()
                    .configure(
                        config_app(app_state.clone()))
            ).await;
            let req = test::TestRequest::post()
                .header("Content-Type", "application/trig")
                .uri("/store")
                .set_payload("<http://example.com>")
                .to_request();
            let resp = test::call_service(&mut app, req).await;
            assert_eq!(resp.status(), http::StatusCode::BAD_REQUEST);
        }

    }

    mod query {
        use super::*;

        #[actix_rt::test]
        async fn get_query() {
            let path = tempdir().unwrap();
            let app_state = web::Data::new(
                AppState {
                    store: SledStore::open(path.path()).unwrap()
                }
            );
            let mut app = test::init_service(
                App::new()
                    .configure(
                        config_app(app_state.clone()))
            ).await;
            let req = test::TestRequest::get()
                .uri(
                    "http://localhost/query?query=SELECT%20*%20WHERE%20{%20?s%20?p%20?o%20}"
                )
                .to_request();
            let resp = test::call_service(&mut app, req).await;
            assert_eq!(resp.status(), http::StatusCode::OK);
        }

        #[actix_rt::test]
        async fn get_query_named_graph() {
            let path = tempdir().unwrap();
            let app_state = web::Data::new(
                AppState {
                    store: SledStore::open(path.path()).unwrap()
                }
            );
            let mut app = test::init_service(
                App::new()
                    .configure(
                        config_app(app_state.clone()))
            ).await;
            let req = test::TestRequest::get()
                .uri(
                    "http://localhost/query?query=SELECT%20*%20WHERE%20{%20?s%20?p%20?o%20}&named-graph-uri=http://example.com/a&named-graph-uri=http://example.com/b"
                )
                .to_request();
            let resp = test::call_service(&mut app, req).await;
            assert_eq!(resp.status(), http::StatusCode::OK);
        }

        #[actix_rt::test]
        async fn get_query_default_graph() {
            let path = tempdir().unwrap();
            let app_state = web::Data::new(
                AppState {
                    store: SledStore::open(path.path()).unwrap()
                }
            );
            let mut app = test::init_service(
                App::new()
                    .configure(
                        config_app(app_state.clone()))
            ).await;
            let req = test::TestRequest::get()
                .uri(
                    "http://localhost/query?query=SELECT%20*%20WHERE%20{%20?s%20?p%20?o%20}&default-graph-uri=http://example.com/a&default-graph-uri=http://example.com/b"
                )
                .to_request();
            let resp = test::call_service(&mut app, req).await;
            assert_eq!(resp.status(), http::StatusCode::OK);
        }

        #[actix_rt::test]
        async fn get_bad_query() {
            let path = tempdir().unwrap();
            let app_state = web::Data::new(
                AppState {
                    store: SledStore::open(path.path()).unwrap()
                }
            );
            let mut app = test::init_service(
                App::new()
                    .configure(
                        config_app(app_state.clone()))
            ).await;
            let req = test::TestRequest::get()
                .uri("http://localhost/query?query=SELECT")
                .to_request();
            let resp = test::call_service(&mut app, req).await;
            assert_eq!(resp.status(), http::StatusCode::BAD_REQUEST);
        }

        #[actix_rt::test]
        async fn get_without_query() {
            let path = tempdir().unwrap();
            let app_state = web::Data::new(
                AppState {
                    store: SledStore::open(path.path()).unwrap()
                }
            );
            let mut app = test::init_service(
                App::new()
                    .configure(
                        config_app(app_state.clone()))
            ).await;
            let req = test::TestRequest::get()
                .uri("http://localhost/query")
                .to_request();
            let resp = test::call_service(&mut app, req).await;
            assert_eq!(resp.status(), http::StatusCode::BAD_REQUEST);
        }

        #[actix_rt::test]
        async fn post_query() {
            let path = tempdir().unwrap();
            let app_state = web::Data::new(
                AppState {
                    store: SledStore::open(path.path()).unwrap()
                }
            );
            let mut app = test::init_service(
                App::new()
                    .configure(
                        config_app(app_state.clone()))
            ).await;
            let req = test::TestRequest::post()
                .uri("http://localhost/query")
                .header("Content-type", "application/sparql-query")
                .set_payload("SELECT * WHERE { ?s ?p ?o }")
                .to_request();
            let resp = test::call_service(&mut app, req).await;
            println!("response: {:?}", resp.response().body().as_ref());
            assert_eq!(resp.status(), http::StatusCode::OK);
        }

        #[actix_rt::test]
        async fn post_bad_query() {
            let path = tempdir().unwrap();
            let app_state = web::Data::new(
                AppState {
                    store: SledStore::open(path.path()).unwrap()
                }
            );
            let mut app = test::init_service(
                App::new()
                    .configure(
                        config_app(app_state.clone()))
            ).await;
            let req = test::TestRequest::post()
                .uri("http://localhost/query")
                .header("Content-type", "application/sparql-query")
                .set_payload("SELECT")
                .to_request();
            let resp = test::call_service(&mut app, req).await;
            assert_eq!(resp.status(), http::StatusCode::BAD_REQUEST);
        }

        #[actix_rt::test]
        async fn post_unknown_query() {
            let path = tempdir().unwrap();
            let app_state = web::Data::new(
                AppState {
                    store: SledStore::open(path.path()).unwrap()
                }
            );
            let mut app = test::init_service(
                App::new()
                    .configure(
                        config_app(app_state.clone()))
            ).await;
            let req = test::TestRequest::post()
                .uri("http://localhost/query")
                .header("Content-type", "application/sparql-todo")
                .set_payload("SELECT")
                .to_request();
            let resp = test::call_service(&mut app, req).await;
            assert_eq!(resp.status(), http::StatusCode::UNSUPPORTED_MEDIA_TYPE);
        }

        #[actix_rt::test]
        async fn post_query_no_content_type() {
            let path = tempdir().unwrap();
            let app_state = web::Data::new(
                AppState {
                    store: SledStore::open(path.path()).unwrap()
                }
            );
            let mut app = test::init_service(
                App::new()
                    .configure(
                        config_app(app_state.clone()))
            ).await;
            let req = test::TestRequest::post()
                .uri("http://localhost/query")
                .set_payload("SELECT")
                .to_request();
            let resp = test::call_service(&mut app, req).await;
            assert_eq!(resp.status(), http::StatusCode::BAD_REQUEST);
        }

        #[actix_rt::test]
        async fn post_federated_query() {
            let path = tempdir().unwrap();
            let app_state = web::Data::new(
                AppState {
                    store: SledStore::open(path.path()).unwrap()
                }
            );
            let mut app = test::init_service(
                App::new()
                    .configure(
                        config_app(app_state.clone()))
            ).await;
            let req = test::TestRequest::post()
                .uri("http://localhost/query")
                .header("Content-type", "application/sparql-query")
                .set_payload("SELECT * WHERE { SERVICE <https://query.wikidata.org/sparql> { <https://en.wikipedia.org/wiki/Paris> ?p ?o } }")
                .to_request();
            let resp = test::call_service(&mut app, req).await;
            assert_eq!(resp.status(), http::StatusCode::OK);
        }

        #[actix_rt::test]
        async fn post_query_form() {
            let path = tempdir().unwrap();
            let app_state = web::Data::new(
                AppState {
                    store: SledStore::open(path.path()).unwrap()
                }
            );
            let mut app = test::init_service(
                App::new()
                    .configure(
                        config_app(app_state.clone()))
            ).await;
            let req = test::TestRequest::get()
                .uri("http://localhost/query")
                .header("Content-Type", "application/x-www-urlencoded")
                .set_payload("query=SELECT%20*%20WHERE%20{%20?s%20?p%20?o%20}")
                .to_request();
            let resp = test::call_service(&mut app, req).await;
            assert_eq!(resp.status(), http::StatusCode::OK);
        }
    }

    mod update {
        use super::*;

        #[actix_rt::test]
        async fn post_update() {
            let path = tempdir().unwrap();
            let app_state = web::Data::new(
                AppState {
                    store: SledStore::open(path.path()).unwrap()
                }
            );
            let mut app = test::init_service(
                App::new()
                    .configure(
                        config_app(app_state.clone()))
            ).await;
            let req = test::TestRequest::post()
                .uri("http://localhost/update")
                .header("Content-Type", "application/sparql-update")
            .set_payload(
            "INSERT DATA { <http://example.com> <http://example.com> <http://example.com> }")
            .to_request();
            let resp = test::call_service(&mut app, req).await;
            assert_eq!(resp.status(), http::StatusCode::NO_CONTENT);
    }

        #[actix_rt::test]
        async fn post_bad_update() {
            let path = tempdir().unwrap();
            let app_state = web::Data::new(
                AppState {
                    store: SledStore::open(path.path()).unwrap()
                }
            );
            let mut app = test::init_service(
                App::new()
                    .configure(
                        config_app(app_state.clone()))
            ).await;
            let req = test::TestRequest::post()
                .uri("http://localhost/update")
                .header("Content-Type", "application/sparql-update")
                .set_payload("INSERT")
                .to_request();
            let resp = test::call_service(&mut app, req).await;
            assert_eq!(resp.status(), http::StatusCode::BAD_REQUEST);
        }
    }

    #[actix_rt::test]
    async fn graph_store_protocol() {
        // Tests from https://www.w3.org/2009/sparql/docs/tests/data-sparql11/http-rdf-update/

        let path = tempdir().unwrap();
        let app_state = web::Data::new(
            AppState {
                store: SledStore::open(path.path()).unwrap()
            }
        );
        let mut app = test::init_service(
            App::new()
                .configure(
                    config_app(app_state.clone()))
        ).await;

        // PUT - Initial state
        println!("PUT - Initial state");
        let req = test::TestRequest::put()
            .uri("http://localhost/store/person/1.ttl")
            .header("Content-Type", "text/turtle; charset=utf-8")
            .set_payload(
            "
@prefix foaf: <http://xmlns.com/foaf/0.1/> .
@prefix v: <http://www.w3.org/2006/vcard/ns#> .

<http://$HOST$/$GRAPHSTORE$/person/1> a foaf:Person;
    foaf:businessCard [
        a v:VCard;
        v:fn \"John Doe\"
    ].
")
            .to_request();
        let resp = test::call_service(&mut app, req).await;
        assert_eq!(resp.status(), http::StatusCode::CREATED);

        // HEAD on an existing graph
        println!("HEAD on an existing graph");
        let req = test::TestRequest::default()
            .method(http::Method::HEAD)
            .uri("http://localhost/store/person/1.ttl")
            .to_request();
        let resp = test::call_service(&mut app, req).await;
        assert_eq!(resp.status(), http::StatusCode::OK);

        // HEAD on an non-existing graph
        println!("HEAD on an non-existing graph");
        let req = test::TestRequest::default()
            .method(http::Method::HEAD)
            .uri("http://localhost/store/person/4.ttl")
            .to_request();
        let resp = test::call_service(&mut app, req).await;
        assert_eq!(resp.status(), http::StatusCode::NOT_FOUND);

        // PUT - graph already in store
        println!("PUT - graph already in store");
        let req = test::TestRequest::put()
            .uri("http://localhost/store/person/1.ttl")
            .header("Content-Type", "text/turtle; charset=utf-8")
            .set_payload(
            "
@prefix foaf: <http://xmlns.com/foaf/0.1/> .
@prefix v: <http://www.w3.org/2006/vcard/ns#> .

<http://$HOST$/$GRAPHSTORE$/person/1> a foaf:Person;
    foaf:businessCard [
        a v:VCard;
        v:fn \"Jane Doe\"
    ].
")
            .to_request();
        let resp = test::call_service(&mut app, req).await;
        assert_eq!(resp.status(), http::StatusCode::NO_CONTENT);

        // GET of PUT - graph already in store
        println!("GET of PUT - graph already in store");
        let req = test::TestRequest::get()
            .uri("http://localhost/store/person/1.ttl")
            .header("Accept", "text/turtle")
            .to_request();
        let resp = test::call_service(&mut app, req).await;
        assert_eq!(resp.status(), http::StatusCode::OK);

        // PUT - default graph
        println!("PUT - default graph");
        let req = test::TestRequest::put()
            .uri("http://localhost/store?default")
            .header("Content-Type", "text/turtle; charset=utf-8")
            .set_payload(
            "
@prefix foaf: <http://xmlns.com/foaf/0.1/> .
@prefix v: <http://www.w3.org/2006/vcard/ns#> .

[]  a foaf:Person;
    foaf:businessCard [
        a v:VCard;
        v:given-name \"Alice\"
    ] .
")
            .to_request();
        let resp = test::call_service(&mut app, req).await;
        assert_eq!(resp.status(), http::StatusCode::NO_CONTENT); // The default graph always exists in Oxigraph

        // GET of PUT - default graph
        println!("GET of PUT - default graph");
        let req = test::TestRequest::get()
            .uri("http://localhost/store?default")
            .header("Accept", "text/turtle")
            .to_request();
        let resp = test::call_service(&mut app, req).await;
        assert_eq!(resp.status(), http::StatusCode::OK);

        // PUT - mismatched payload
        println!("PUT - mismatched payload");
        let req = test::TestRequest::put()
            .uri("http://localhost/store/person/1.ttl")
            .header("Content-Type", "text/turtle; charset=utf-8")
            .set_payload("@prefix fo")
            .to_request();
        let resp = test::call_service(&mut app, req).await;
        assert_eq!(resp.status(), http::StatusCode::BAD_REQUEST);

        // PUT - empty graph
        println!("PUT - empty graph");
        let req = test::TestRequest::put()
            .uri("http://localhost/store/person/2.ttl")
            .header("Content-Type", "text/turtle; charset=utf-8")
            .to_request();
        let resp = test::call_service(&mut app, req).await;
        assert_eq!(resp.status(), http::StatusCode::CREATED);

        // GET of PUT - empty graph
        println!("GET of PUT - empty graph");
        let req = test::TestRequest::get()
            .uri("http://localhost/store/person/2.ttl")
            .header("Accept", "text/turtle")
            .to_request();
        let resp = test::call_service(&mut app, req).await;
        assert_eq!(resp.status(), http::StatusCode::OK);

        // PUT - replace empty graph
        println!("PUT - replace empty graph");
        let req = test::TestRequest::put()
            .uri("http://localhost/store/person/2.ttl")
            .header("Content-Type", "text/turtle; charset=utf-8")
            .set_payload(
            "
@prefix foaf: <http://xmlns.com/foaf/0.1/> .
@prefix v: <http://www.w3.org/2006/vcard/ns#> .

[]  a foaf:Person;
    foaf:businessCard [
        a v:VCard;
        v:given-name \"Alice\"
    ] .
")
            .to_request();
        let resp = test::call_service(&mut app, req).await;
        assert_eq!(resp.status(), http::StatusCode::NO_CONTENT);

        // GET of replacement for empty graph
        let req = test::TestRequest::get()
            .uri("http://localhost/store/person/2.ttl")
            .header("Accept", "text/turtle")
            .to_request();
        let resp = test::call_service(&mut app, req).await;
        assert_eq!(resp.status(), http::StatusCode::OK);

        // DELETE - existing graph
        println!("DELETE - existing graph");
        let req = test::TestRequest::delete()
            .uri("http://localhost/store/person/2.ttl")
            .to_request();
        let resp = test::call_service(&mut app, req).await;
        assert_eq!(resp.status(), http::StatusCode::NO_CONTENT);

        // GET of DELETE - existing graph
        let req = test::TestRequest::get()
            .uri("http://localhost/store/person/2.ttl")
            .to_request();
        let resp = test::call_service(&mut app, req).await;
        assert_eq!(resp.status(), http::StatusCode::NOT_FOUND);

        // DELETE - non-existent graph
        let req = test::TestRequest::delete()
            .uri("http://localhost/store/person/2.ttl")
            .to_request();
        let resp = test::call_service(&mut app, req).await;
        assert_eq!(resp.status(), http::StatusCode::NOT_FOUND);

        // POST - existing graph
        let req = test::TestRequest::post()
            .uri("http://localhost/store/person/1.ttl")
            .header("Content-Type", "text/turtle; charset=utf-8")
            .to_request();
        let resp = test::call_service(&mut app, req).await;
        assert_eq!(resp.status(), http::StatusCode::NO_CONTENT);

        // TODO: POST - multipart/form-data
        // TODO: GET of POST - multipart/form-data

        // POST - create new graph
        let req = test::TestRequest::post()
            .uri("http://localhost/store")
            .header("Content-Type", "text/turtle; charset=utf-8")
            .set_payload(
            "
@prefix foaf: <http://xmlns.com/foaf/0.1/> .
@prefix v: <http://www.w3.org/2006/vcard/ns#> .

[]  a foaf:Person;
    foaf:businessCard [
        a v:VCard;
        v:given-name \"Alice\"
    ] .
")
            .to_request();
        let resp = test::call_service(&mut app, req).await;
        assert_eq!(resp.status(), http::StatusCode::CREATED);
        let location = resp.headers().get("Location").unwrap().to_str().unwrap();

        // GET of POST - create new graph
        let req = test::TestRequest::get()
            .uri(location)
            .header("Accept", "text/turtle")
            .to_request();
        let resp = test::call_service(&mut app, req).await;
        assert_eq!(resp.status(), http::StatusCode::OK);

        // POST - empty graph to existing graph
        let req = test::TestRequest::post()
            .uri(location)
            .header("Content-Type", "text/turtle; charset=utf-8")
            .to_request();
        let resp = test::call_service(&mut app, req).await;
        assert_eq!(resp.status(), http::StatusCode::NO_CONTENT);

        // GET of POST - after noop
        let req = test::TestRequest::get()
            .uri(location)
            .header("Accept", "text/turtle")
            .to_request();
        let resp = test::call_service(&mut app, req).await;
        assert_eq!(resp.status(), http::StatusCode::OK);


    }
}
