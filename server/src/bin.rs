use oxigraph::SledStore;
use actix_files::NamedFile;
use actix_web::{
    get,
    post,
    web,
    HttpRequest,
    HttpResponse,
    Responder,
    Result
};
use serde_derive::Deserialize;
use std::path::PathBuf;

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    use actix_web::{App, HttpServer};
    env_logger::init();
    println!("Starting server on 127.0.0.1:8080 ...");
    let store = SledStore::open("example.db");

    HttpServer::new(|| {
        App::new()
            .service(
                web::resource("/")
                    .route(web::get().to(get_index))
            )
            .service(get_query)
            .service(post_store)
    })
    .bind(("127.0.0.1", 8080))?
    .run()
    .await

}

// #[get("/")]
async fn get_index() -> Result<NamedFile> {
    let path = PathBuf::from("templates/index.html");
    Ok(NamedFile::open(path)?)
}

#[get("/query")]
async fn get_query(form: web::Query<QueryData>) -> impl Responder {
    log::info!("query: {:?}", form);
    format!("get_query: {:?}", form)
}

#[post("/store")]
async fn post_store(req: HttpRequest) -> impl Responder {
    if let Some(content_type) = req.headers().get("content-type") {
        HttpResponse::Ok().body(format!("target: {}\ncontent-type: {:?}", req.query_string(), content_type))
    } else {
        HttpResponse::BadRequest().body("No Content-Type given.")
    }
}

#[derive(Deserialize, Debug)]
struct QueryData {
    query: String,
    default_graph_uri: Vec<String>,
    named_graph_uri: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use actix_web::{http, test, App};

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

    #[actix_rt::test]
    async fn post_dataset_file() {
        let mut app = test::init_service(
            App::new()
                .service(
                    web::resource("/")
                        .route(web::get().to(get_index))
                )
        ).await;
        let req = test::TestRequest::post()
            .uri("/store")
            .header("Content-Type", "application/trig")
            .set_payload("<http://example.com> <http://example.com> <http://example.com> .")
            .to_request();
        let resp = test::call_service(&mut app, req).await;
        assert_eq!(resp.status(), http::StatusCode::NO_CONTENT);
    }
}
