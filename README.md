# Rust Client for TairVector

[Tair](https://www.alibabacloud.com/product/tair) is a redis compitable in-memory database developed by Alibaba Cloud. The [TairVector](https://www.alibabacloud.com/help/en/tair/latest/tairvector) extension provides the capabilities of storing high dimensional feature vectors and fast searching for approximate nearest neighbors.

This repo is an **unofficial** rust driver for TairVector. It's a simple wrapper of redis-rs with TairVector specific interfaces.

Cluster and pipeline are not supported at present.

## Usage

```rust
use tair_vector_rs::{TairVectorCommands, Vector};

fn main() {
    let url = "YOUR_TAIR_URL";
    let client = redis::Client::open(url).unwrap();
    let mut conn = client.get_connection().unwrap();

    let index_name = "test-index";
    let _: bool = conn.tvs_create_index(index_name, 2, "FLAT", "L2").unwrap();

    let vector1: Vector = Vector { 0: vec![1.0, 2.0] };
    let vector2: Vector = Vector { 0: vec![3.0, 4.0] };
    let vector3: Vector = Vector { 0: vec![5.0, 6.0] };
    let _: usize = conn
        .tvs_hset_multi(
            index_name,
            "k1",
            &vec![("attr1", "val1"), ("VECTOR", &vector1.to_string())],
        )
        .unwrap();

    let _: usize = conn.tvs_hset(index_name, "k2", "VECTOR", vector2).unwrap();

    let _: usize = conn.tvs_hset_vector(index_name, "k3", vector3).unwrap();

    let query: Vector = Vector { 0: vec![0.0, 0.0] };
    let knn_results: Vec<(String, f32)> = conn.tvs_knnsearch(index_name, 10, &query).unwrap();
    println!("knn results: {:?}", knn_results);
}
```
