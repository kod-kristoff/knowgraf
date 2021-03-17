use oxigraph::SledStore;
use oxigraph::io::DatasetFormat;
use oxigraph::io::GraphFormat;
use actix_files::NamedFile;
use actix_web::{
    get,
    web,
    HttpRequest,
    HttpResponse,
    Responder,
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
            .service(get_query)
            .service(
                web::resource("/store")
                    .route(web::post().to(post_store))
            );
    })
}

// #[get("/")]
async fn get_index() -> Result<NamedFile, AppError> {
    let path = PathBuf::from("templates/index.html");
    Ok(NamedFile::open(path)?)
}

#[get("/query")]
async fn get_query(form: web::Query<QueryData>) -> impl Responder {
    log::info!("query: {:?}", form);
    format!("get_query: {:?}", form)
}

// #[post("/store")]
async fn post_store(req: HttpRequest, data: web::Data<AppState>, body: String) -> Result<HttpResponse, AppError> {
    if let Some(content_type) = req.headers().get("content-type") {
        let content_type = content_type.to_str().unwrap();
        println!("content-type: {:?}", content_type);
        println!("graphformat: {:?}", GraphFormat::from_media_type(content_type));
        println!("datasetformat: {:?}", DatasetFormat::from_media_type(content_type));
        if let Some(format) = DatasetFormat::from_media_type(content_type) {
            data.store
                .load_dataset(
                    io::BufReader::new(io::Cursor::new(body)),
                    format,
                    None
                )?;
            return Ok(HttpResponse::NoContent().finish());
        } else {
            println!("no supported media type");
            Ok(HttpResponse::UnsupportedMediaType().body(format!("No supported Content-Type given: {}", content_type)))
        }
        // Ok(HttpResponse::Ok().body(format!("target: {}\ncontent-type: {:?}", req.query_string(), content_type)))
    } else {
        println!("no content type");
        Ok(HttpResponse::BadRequest().body("No Content-Type given."))
    }
}

#[derive(Deserialize, Debug)]
struct QueryData {
    query: String,
    default_graph_uri: Vec<String>,
    named_graph_uri: String,
}

#[derive(Debug, Display, Error)]
enum AppError {
    #[display(fmt = "io error")]
    IoError(io::Error),
}

impl error::ResponseError for AppError {
    fn error_response(&self) -> HttpResponse {
        use actix_web::dev::HttpResponseBuilder;
        HttpResponseBuilder::new(self.status_code())
            .set_header(http::header::CONTENT_TYPE, "text/html; charset=utf-8")
            .body(self.to_string())
    }

    fn status_code(&self) -> http::StatusCode {
        match *self {
            _ => http::StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

impl From<io::Error> for AppError {
    fn from(err: io::Error) -> AppError {
        AppError::IoError(err)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use actix_web::{http, test, App};
    use tempfile::{tempdir};

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
}
