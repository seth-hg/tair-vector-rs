#[cfg(feature = "ndarray")]
use ndarray::prelude::*;
#[cfg(feature = "aio")]
use redis::{AsyncIter, RedisFuture};
use redis::{
    ConnectionLike, ErrorKind, FromRedisValue, Iter, RedisError, RedisResult, RedisWrite,
    ToRedisArgs, Value,
};
use std::fmt;
use std::str;

#[macro_use]
pub mod macros;

#[cfg(feature = "bulk")]
mod bulk;

#[cfg(feature = "bulk")]
pub use crate::bulk::BulkOps;

implement_commands! {
    'a

    /// TVS.CREATEINDEX index_name dimension HNSW/FLAT L2/IP/COSINE
    fn tvs_create_index<K: ToRedisArgs, D: ToRedisArgs, IT: ToRedisArgs, DT: ToRedisArgs>(
        index_name: K,
        dim: D,
        index_type: IT,
        distance_type: DT
    ) {
        redis::cmd("TVS.CREATEINDEX")
            .arg(index_name)
            .arg(dim)
            .arg(index_type)
            .arg(distance_type)
    }

    /// TVS.CREATEINDEX index_name dimension HNSW L2/IP/COSINE [ef_construct 500] [M 16]
    fn tvs_create_index_with_params<
        K: ToRedisArgs,
        PK: ToRedisArgs,
        PV: ToRedisArgs
    >(
        index_name: K,
        dim: usize,
        index_type: &'a str,
        distance_type: &'a str,
        params: &'a [(PK, PV)]
    ) {
        redis::cmd("TVS.CREATEINDEX")
            .arg(index_name)
            .arg(dim)
            .arg(index_type)
            .arg(distance_type)
            .arg(params)
    }

    /// TVS.GETINDEX index_name
    fn tvs_get_index<K: ToRedisArgs>(
        index_name: K
    ) {
        redis::cmd("TVS.GETINDEX").arg(index_name)
    }

    /// TVS.DELINDEX index_name
    fn tvs_del_index<K: ToRedisArgs>(index_name: K) {
        redis::cmd("TVS.DELINDEX").arg(index_name)
    }

    /// TVS.HSET index_name key VECTOR vector
    fn tvs_hset_vector<IK: ToRedisArgs, VK: ToRedisArgs, V: ToRedisArgs>(
        index_name: IK,
        key: VK,
        vector: V
    )  {
        redis::cmd("TVS.HSET")
            .arg(index_name)
            .arg(key)
            .arg("VECTOR")
            .arg(vector)
    }

    /// TVS.HSET index_name key field value
    fn tvs_hset<IK: ToRedisArgs, VK: ToRedisArgs, FK: ToRedisArgs, FV: ToRedisArgs>(
        index_name: IK,
        key: VK,
        attr: FK,
        value: FV
    ) {
        redis::cmd("TVS.HSET")
            .arg(index_name)
            .arg(key)
            .arg(attr)
            .arg(value)
    }

    /// TVS.HSET index_name key [field1 val1]...
    fn tvs_hset_multi<IK: ToRedisArgs, VK: ToRedisArgs, FK: ToRedisArgs, FV: ToRedisArgs>(
        index_name: IK,
        key: VK,
        attrs: &'a [(FK, FV)]
    ) {
        redis::cmd("TVS.HSET")
            .arg(index_name)
            .arg(key)
            .arg(attrs)
    }

    /// TVS.HGETALL index_name key
    fn tvs_hgetall<IK: ToRedisArgs, VK: ToRedisArgs>(
        index_name: IK,
        key: VK
    ) {
        redis::cmd("TVS.HGETALL")
            .arg(index_name)
            .arg(key)
    }

    /// TVS.HMGET index_name key VECTOR
    fn tvs_get_vector<IK: ToRedisArgs, VK: ToRedisArgs>(
        index_name: IK,
        key: VK
    ) {
        redis::cmd("TVS.HMGET")
            .arg(index_name)
            .arg(key)
            .arg("VECTOR")
    }

    /// TVS.HMGET index_name key1...
    fn tvs_hmget<IK: ToRedisArgs, VK: ToRedisArgs, F: ToRedisArgs>(
        index_name: IK,
        key: VK,
        attrs: &'a [F]
    ) {
        redis::cmd("TVS.HMGET")
            .arg(index_name)
            .arg(key)
            .arg(attrs)
    }

    /// TVS.KNNSEARCH index_name topk vector
    fn tvs_knnsearch<K: ToRedisArgs, V: ToRedisArgs>(index_name: K, topk: usize, vector: V) {
        redis::cmd("TVS.KNNSEARCH")
            .arg(index_name)
            .arg(topk)
            .arg(vector)
    }

    /// TVS.KNNSEARCH index_name topk vector ef_search 200
    fn tvs_knnsearch_with_params<K: ToRedisArgs, V: ToRedisArgs, PK: ToRedisArgs, PV: ToRedisArgs>(
        index_name: K,
        topk: usize,
        vector: V,
        params: &'a [(PK, PV)]
    ) {
        redis::cmd("TVS.KNNSEARCH")
            .arg(index_name)
            .arg(topk)
            .arg(vector)
            .arg(params)
    }
}

impl<T> TairVectorCommands for T where T: ConnectionLike {}
#[cfg(feature = "aio")]
impl<T> TairVectorAsyncCommands for T where T: redis::aio::ConnectionLike + Send + Sized {}

pub struct Vector(Vec<f32>);

trait VectorToRedisArgs: ToRedisArgs {}

impl VectorToRedisArgs for Vec<f32> {}

#[cfg(feature = "ndarray")]
pub struct NdArrayVector<'a>(ArrayView1<'a, f32>);

impl fmt::Display for Vector {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[")?;
        for (i, value) in self.0.iter().enumerate() {
            if i > 0 {
                write!(f, ",")?;
            }
            write!(f, "{}", value)?;
        }
        write!(f, "]")
    }
}

impl ToRedisArgs for Vector {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + RedisWrite,
    {
        let encoded = format!(
            "[{}]",
            self.0
                .iter()
                .map(|x| x.to_string())
                .collect::<Vec<String>>()
                .join(",")
        );
        out.write_arg_fmt(encoded);
    }
}

#[cfg(feature = "ndarray")]
impl ToRedisArgs for NdArrayVector<'_> {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + RedisWrite,
    {
        let encoded = format!(
            "[{}]",
            self.0
                .iter()
                .map(|x| x.to_string())
                .collect::<Vec<String>>()
                .join(",")
        );
        out.write_arg_fmt(encoded);
    }
}

impl From<&[u8]> for Vector {
    fn from(data: &[u8]) -> Self {
        let segments: Vec<&[u8]> = data[1..(data.len() - 1)].split(|&x| x == b',').collect();

        let vector: Vec<f32> = segments
            .iter()
            .filter_map(|segment| std::str::from_utf8(segment).ok())
            .filter_map(|segment| segment.parse::<f32>().ok())
            .collect();
        Vector { 0: vector }
    }
}

impl FromRedisValue for Vector {
    fn from_redis_value(value: &redis::Value) -> RedisResult<Self> {
        match value {
            Value::Data(v) => Ok(Vector::from(v.as_slice())),
            _ => Err(RedisError::from((ErrorKind::TypeError, ""))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(feature = "aio")]
    use futures::stream::StreamExt;
    use redis::Iter;
    use std::collections::HashMap;
    use std::env;

    #[test]
    fn sync_ops() {
        let redis_url = if let Ok(v) = env::var("TAIR_URL") {
            v
        } else {
            String::from("redis://127.0.0.1/")
        };

        let index_name = "test-sync-ops";
        let client = redis::Client::open(redis_url).unwrap();
        let mut conn = client.get_connection().unwrap();

        // cleanup
        conn.tvs_del_index::<_, usize>(index_name).unwrap();

        // TVS.CREATEINDEX
        let params = HashMap::from([("ef_construct", 100), ("M", 24)]);
        // let px: Vec<(&str, i32)> = params.into_iter().collect();
        let created: bool = conn
            .tvs_create_index_with_params(
                index_name,
                2,
                "HNSW",
                "L2",
                &(params.clone().into_iter().collect::<Vec<(&str, i32)>>()),
            )
            .unwrap();
        assert!(created);

        // TVS.SCANINDEX
        let iter: Iter<String> = conn.tvs_scan_index().unwrap();
        let scanned_indices: Vec<String> = iter.collect();
        assert!(scanned_indices.len() >= 1);
        let mut found = false;
        for name in scanned_indices {
            if name == index_name {
                found = true;
            }
        }
        assert!(found);

        let iter: Iter<String> = conn.tvs_scan_index_match(index_name).unwrap();
        let scanned_indices: Vec<String> = iter.collect();
        assert_eq!(scanned_indices.len(), 1);
        assert_eq!(scanned_indices[0], index_name);

        // TVS.GETINDEX
        let index_info: HashMap<String, String> = conn.tvs_get_index(index_name).unwrap();
        assert!(index_info.len() > 0);
        for (k, v) in params {
            assert_eq!(index_info.get(k).unwrap().to_owned(), v.to_string());
        }

        // TVS.DELETEINDEX
        let deleted: usize = conn.tvs_del_index(index_name).unwrap();
        assert_eq!(deleted, 1);

        let created: bool = conn.tvs_create_index(index_name, 2, "FLAT", "L2").unwrap();
        assert!(created);

        // TVS.HSET
        let vector: Vector = Vector { 0: vec![1.0, 2.0] };
        let count: usize = conn
            .tvs_hset_multi(
                index_name,
                "k1",
                &vec![("attr1", "val1"), ("VECTOR", &vector.to_string())],
            )
            .unwrap();
        assert_eq!(count, 2);

        // TVS.HGETALL
        let got: HashMap<String, String> = conn.tvs_hgetall(index_name, "k1").unwrap();
        assert_eq!(got.len(), 2);
        assert_eq!(got.get("attr1").unwrap(), "val1");
        assert_eq!(got.get("VECTOR").unwrap(), "[1,2]");

        // TVS.HMGET
        let got: Vec<String> = conn
            .tvs_hmget(index_name, "k1", &["attr1", "VECTOR"])
            .unwrap();
        assert_eq!(got.len(), 2);
        assert_eq!(got[0], "val1");
        assert_eq!(got[1], "[1,2]");

        let got_vector: Vec<Vector> = conn.tvs_get_vector(index_name, "k1").unwrap();
        assert_eq!(got_vector.len(), 1);
        // assert_eq!(got_vector[0].0.len(), 2);

        // TVS.SCAN
        let key_iter: Iter<String> = conn.tvs_scan(index_name).unwrap();
        let scanned_keys: Vec<String> = key_iter.collect();
        assert_eq!(scanned_keys.len(), 1);
        assert_eq!(scanned_keys[0], "k1");

        // TVS.KNNSEARCH
        let knn_results: Vec<(String, f32)> = conn
            .tvs_knnsearch_with_params(index_name, 10, &vector, &vec![("ef_search", 100)])
            .unwrap();
        assert_eq!(knn_results.len(), 1);
        assert_eq!(knn_results[0].0, "k1");
        assert_eq!(knn_results[0].1, 0.0);

        conn.tvs_del_index::<_, usize>(index_name).unwrap();
    }

    #[cfg(feature = "aio")]
    #[tokio::test]
    async fn async_ops() {
        let redis_url = if let Ok(v) = env::var("TAIR_URL") {
            v
        } else {
            String::from("redis://127.0.0.1/")
        };

        let index_name = "test-async-ops";
        let client = redis::Client::open(redis_url).unwrap();
        let mut conn = client.get_async_connection().await.unwrap();

        // cleanup
        conn.tvs_del_index::<_, usize>(index_name).await.unwrap();

        // TVS.CREATEINDEX
        let params = HashMap::from([("ef_construct", 100), ("M", 24)]);
        // let px: Vec<(&str, i32)> = params.into_iter().collect();
        let created: bool = conn
            .tvs_create_index_with_params(
                index_name,
                2,
                "HNSW",
                "L2",
                &(params.clone().into_iter().collect::<Vec<(&str, i32)>>()),
            )
            .await
            .unwrap();
        assert!(created);

        // TVS.SCANINDEX
        let iter: AsyncIter<String> = conn.tvs_scan_index().await.unwrap();
        let scanned_indices: Vec<String> = iter.collect().await;
        assert!(scanned_indices.len() >= 1);
        let mut found = false;
        for name in scanned_indices {
            if name == index_name {
                found = true;
            }
        }
        assert!(found);

        let iter: AsyncIter<String> = conn.tvs_scan_index_match(index_name).await.unwrap();
        let scanned_indices: Vec<String> = iter.collect().await;
        assert_eq!(scanned_indices.len(), 1);
        assert_eq!(scanned_indices[0], index_name);

        // TVS.GETINDEX
        let index_info: HashMap<String, String> = conn.tvs_get_index(index_name).await.unwrap();
        assert!(index_info.len() > 0);
        for (k, v) in params {
            assert_eq!(index_info.get(k).unwrap().to_owned(), v.to_string());
        }

        // TVS.DELETEINDEX
        let deleted: usize = conn.tvs_del_index(index_name).await.unwrap();
        assert_eq!(deleted, 1);

        let created: bool = conn
            .tvs_create_index(index_name, 2, "FLAT", "L2")
            .await
            .unwrap();
        assert!(created);

        // TVS.HSET
        let vector: Vector = Vector { 0: vec![1.0, 2.0] };
        let count: usize = conn
            .tvs_hset_multi(
                index_name,
                "k1",
                &vec![("attr1", "val1"), ("VECTOR", &vector.to_string())],
            )
            .await
            .unwrap();
        assert_eq!(count, 2);

        // TVS.HGETALL
        let got: HashMap<String, String> = conn.tvs_hgetall(index_name, "k1").await.unwrap();
        assert_eq!(got.len(), 2);
        assert_eq!(got.get("attr1").unwrap(), "val1");
        assert_eq!(got.get("VECTOR").unwrap(), "[1,2]");

        // TVS.HMGET
        let got: Vec<String> = conn
            .tvs_hmget(index_name, "k1", &["attr1", "VECTOR"])
            .await
            .unwrap();
        assert_eq!(got.len(), 2);
        assert_eq!(got[0], "val1");
        assert_eq!(got[1], "[1,2]");

        let got_vector: Vec<Vector> = conn.tvs_get_vector(index_name, "k1").await.unwrap();
        assert_eq!(got_vector.len(), 1);
        // assert_eq!(got_vector[0].0.len(), 2);

        // TVS.SCAN
        let key_iter: AsyncIter<String> = conn.tvs_scan(index_name).await.unwrap();
        let scanned_keys: Vec<String> = key_iter.collect().await;
        assert_eq!(scanned_keys.len(), 1);
        assert_eq!(scanned_keys[0], "k1");

        // TVS.KNNSEARCH
        let knn_results: Vec<(String, f32)> = conn
            .tvs_knnsearch_with_params(index_name, 10, &vector, &vec![("ef_search", 100)])
            .await
            .unwrap();
        assert_eq!(knn_results.len(), 1);
        assert_eq!(knn_results[0].0, "k1");
        assert_eq!(knn_results[0].1, 0.0);

        conn.tvs_del_index::<_, usize>(index_name).await.unwrap();
    }
}
