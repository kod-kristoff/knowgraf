use oxigraph::SledStore;
use actix_files::NamedFile;
use actix_web::{get, post, web, HttpRequest, Responder, Result};
use std::path::PathBuf;

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    use actix_web::{App, HttpServer};
    println!("Starting server on 127.0.0.1:8080 ...");
    let store = SledStore::open("example.db");

    HttpServer::new(|| {
        App::new()
            .service(get_index)
            .service(post_store)
    })
    .bind(("127.0.0.1", 8080))?
    .run()
    .await

}

#[get("/")]
async fn get_index() -> Result<NamedFile> {
    let path = PathBuf::from("templates/index.html");
    Ok(NamedFile::open(path)?)
}

#[post("/store")]
async fn post_store(req: HttpRequest) -> impl Responder {
    format!("target: {}", req.query_string())
}
