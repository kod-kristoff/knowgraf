use oxigraph::SledStore;

fn main() {
    println!("main");
    let store = SledStore::open("example.db");
}
