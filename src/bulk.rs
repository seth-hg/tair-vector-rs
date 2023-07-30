use ndarray::parallel::prelude::*;
use ndarray::prelude::*;
use std::sync::Mutex;

use crate::NdArrayVector;
use crate::TairVectorCommands;

pub trait BulkOps {
    fn bulk_load(&self, index_name: &str, data: &Array2<f32>) -> usize;
}

impl BulkOps for redis::Client {
    fn bulk_load(&self, index_name: &str, data: &Array2<f32>) -> usize {
        // set rayon gloabl thread pool
        // rayon::ThreadPoolBuilder::new()
        //     .num_threads(4)
        //     .build_global()
        //     .unwrap();

        // split data into batches
        let batch_size: usize = 32;
        let batches: Vec<ArrayView2<f32>> = data.axis_chunks_iter(Axis(0), batch_size).collect();

        // create a connection for each thread
        let thread_conns: Vec<Mutex<redis::Connection>> = (0..rayon::current_num_threads())
            .map(|_| Mutex::new(self.get_connection().unwrap()))
            .collect();

        batches
            .par_iter()
            .enumerate()
            .map(|(i, batch)| -> usize {
                let mut conn = thread_conns[rayon::current_thread_index().unwrap()]
                    .lock()
                    .unwrap();
                batch
                    .outer_iter()
                    .enumerate()
                    .map(|(j, v)| -> usize {
                        conn.tvs_hset_vector(
                            index_name,
                            i * batch_size + j,
                            &NdArrayVector { 0: v },
                        )
                        .unwrap()
                    })
                    .sum::<usize>()
            })
            .sum::<usize>()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Vector;
    use ndarray_rand::rand_distr::Uniform;
    use ndarray_rand::RandomExt;
    use redis::Iter;
    use std::collections::HashMap;
    use std::env;

    #[test]
    fn bulk_load() {
        let dim = 32;
        let nvecs = 100;

        // populate test data
        let mut rng = rand::thread_rng();
        let uniform = Uniform::<f32>::new(0.0, 1.0);
        let vecs: Array2<f32> = Array::random_using((nvecs, dim), uniform, &mut rng);

        let redis_url = if let Ok(v) = env::var("TAIR_URL") {
            v
        } else {
            String::from("redis://127.0.0.1/")
        };

        let index_name = "test-bulk-load";
        let client = redis::Client::open(redis_url).unwrap();
        let mut conn = client.get_connection().unwrap();

        // cleanup
        conn.tvs_del_index::<_, usize>(index_name).unwrap();

        let created: bool = conn
            .tvs_create_index(index_name, dim, "FLAT", "L2")
            .unwrap();
        assert!(created);

        let loaded = client.bulk_load(index_name, &vecs);
        assert_eq!(loaded, vecs.nrows());
        let query: Array1<f32> = Array::random_using(dim, uniform, &mut rng);

        let index_info: HashMap<String, String> = conn.tvs_get_index(index_name).unwrap();
        assert!(index_info.len() > 0);
        assert_eq!(
            index_info.get("data_count").unwrap().to_owned(),
            nvecs.to_string()
        );

        let key_iter: Iter<String> = conn.tvs_scan(index_name).unwrap();
        let scanned_keys: Vec<String> = key_iter.collect();
        assert_eq!(scanned_keys.len(), nvecs);
        for key in scanned_keys {
            let idx: usize = key.parse().unwrap();
            let v: Vec<Vector> = conn.tvs_get_vector(index_name, key).unwrap();
            assert_eq!(v.len(), 1);
            assert_eq!(v[0].0.len(), dim);
            for i in 0..dim {
                assert!((v[0].0[i] - vecs.row(idx)[i]).abs() < 1e-6);
            }
        }

        let knn_results: Vec<(String, f32)> = conn
            .tvs_knnsearch(index_name, 10, &NdArrayVector { 0: query.view() })
            .unwrap();
        assert_eq!(knn_results.len(), 10);

        conn.tvs_del_index::<_, usize>(index_name).unwrap();
    }
}
