use oxigraph::SledStore;
use actix_files::NamedFile;
use actix_web::{Responder, Result};
use std::path::PathBuf;

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    use actix_web::{web, App, HttpServer};
    println!("main");
    let store = SledStore::open("example.db");

    HttpServer::new(|| {
        App::new()
            .route("/", web::get().to(index))
    })
    .bind(("127.0.0.1", 8080))?
    .run()
    .await

}

async fn index() -> Result<NamedFile> {
    let path = PathBuf::from("templates/index.html");
    Ok(NamedFile::open(path)?)
}
