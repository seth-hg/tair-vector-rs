macro_rules! implement_commands {
    (
        $lifetime: lifetime
        $(
            $(#[$attr:meta])+
            fn $name:ident<$($tyargs:ident : $ty:ident),*>(
                $($argname:ident: $argty:ty),*) $body:block
        )*
    ) =>
    (
        pub trait TairVectorCommands : ConnectionLike+Sized {
            $(
                $(#[$attr])*
                #[inline]
                #[allow(clippy::extra_unused_lifetimes, clippy::needless_lifetimes)]
                fn $name<$lifetime, $($tyargs: $ty, )* RV: FromRedisValue>(
                    &mut self $(, $argname: $argty)*) -> RedisResult<RV>
                    // { redis::Cmd::$name($($argname),*).query(self) }
                    { ($body).query(self) }
            )*

            #[inline]
            fn tvs_scan_index<K: FromRedisValue>(&mut self) -> RedisResult<Iter<'_, K>> {
                let mut c = redis::cmd("TVS.SCANINDEX");
                c.cursor_arg(0);
                c.iter(self)
            }

            #[inline]
            fn tvs_scan_index_match<P: ToRedisArgs, K: FromRedisValue>(
                &mut self,
                pattern: P,
            ) -> RedisResult<Iter<'_, K>> {
                let mut c = redis::cmd("TVS.SCANINDEX");
                c.arg(0).arg("MATCH").arg(pattern);
                c.iter(self)
            }

            #[inline]
            fn tvs_scan<K: ToRedisArgs, RK: FromRedisValue>(
                &mut self,
                index_name: K,
            ) -> RedisResult<Iter<'_, RK>> {
                let mut c = redis::cmd("TVS.SCAN");
                c.arg(index_name).cursor_arg(0);
                c.iter(self)
            }

            #[inline]
            fn tvs_scan_match<K: ToRedisArgs, P: ToRedisArgs, RK: FromRedisValue>(
                &mut self,
                index_name: K,
                pattern: P,
            ) -> RedisResult<Iter<'_, RK>> {
                let mut c = redis::cmd("TVS.SCAN");
                c.arg(index_name).cursor_arg(0).arg("MATCH").arg(pattern);
                c.iter(self)
            }

            #[inline]
            fn tvs_scan_max_dist<K: ToRedisArgs, V: ToRedisArgs, D: ToRedisArgs, RK: FromRedisValue>(
                &mut self,
                index_name: K,
                vector: &V,
                max_dist: D,
            ) -> RedisResult<Iter<'_, RK>> {
                let mut c = redis::cmd("TVS.SCAN");
                c.arg(index_name)
                    .cursor_arg(0)
                    .arg("VECTOR")
                    .arg(vector)
                    .arg("MAX_DIST")
                    .arg(max_dist);
                c.iter(self)
            }

            #[inline]
            fn tvs_scan_filter<K: ToRedisArgs, F: ToRedisArgs, RK: FromRedisValue>(
                &mut self,
                index_name: K,
                filter: F,
            ) -> RedisResult<Iter<'_, RK>> {
                let mut c = redis::cmd("TVS.SCAN");
                c.arg(index_name).cursor_arg(0).arg("FILTER").arg(filter);
                c.iter(self)
            }

            #[inline]
            fn tvs_scan_full<
                K: ToRedisArgs,
                P: ToRedisArgs,
                V: ToRedisArgs,
                D: ToRedisArgs,
                F: ToRedisArgs,
                RK: FromRedisValue,
            >(
                &mut self,
                index_name: K,
                pattern: Option<P>,
                max_dist: Option<(V, D)>,
                filter: Option<F>,
            ) -> RedisResult<Iter<'_, RK>> {
                let mut c = redis::cmd("TVS.SCAN").arg(index_name).cursor_arg(0).clone();
                if let Some(p) = pattern {
                    c.arg("MATCH").arg(p);
                }
                if let Some((v, d)) = max_dist {
                    c.arg("VECTOR").arg(v).arg("MAX_DIST").arg(d);
                }
                if let Some(f) = filter {
                    c.arg("FILTER").arg(f);
                }

                c.iter(self)
            }

        }

        #[cfg(feature = "aio")]
        pub trait TairVectorAsyncCommands : redis::aio::ConnectionLike + Send + Sized {
            $(
                $(#[$attr])*
                #[inline]
                #[allow(clippy::extra_unused_lifetimes, clippy::needless_lifetimes)]
                fn $name<$lifetime, $($tyargs: $ty + Send + Sync + $lifetime,)* RV>(
                    & $lifetime mut self
                    $(, $argname: $argty)*
                ) -> redis::RedisFuture<'a, RV>
                where
                    RV: FromRedisValue,
                {
                    Box::pin(async move { ($body).query_async(self).await })
                }
            )*

            #[inline]
            fn tvs_scan_index<K: FromRedisValue>(&mut self) -> RedisFuture<AsyncIter<'_, K>> {
                let mut c = redis::cmd("TVS.SCANINDEX");
                c.cursor_arg(0);
                Box::pin(async move { c.iter_async(self).await })
            }

            #[inline]
            fn tvs_scan_index_match<P: ToRedisArgs, K: FromRedisValue>(
                &mut self,
                pattern: P,
            ) -> RedisFuture<AsyncIter<'_, K>> {
                let mut c = redis::cmd("TVS.SCANINDEX");
                c.arg(0).arg("MATCH").arg(pattern);
                Box::pin(async move { c.iter_async(self).await })
            }

            #[inline]
            fn tvs_scan<K: ToRedisArgs, RK: FromRedisValue>(
                &mut self,
                index_name: K,
            ) -> RedisFuture<AsyncIter<'_, RK>> {
                let mut c = redis::cmd("TVS.SCAN");
                c.arg(index_name).cursor_arg(0);
                Box::pin(async move { c.iter_async(self).await })
            }

            #[inline]
            fn tvs_scan_match<K: ToRedisArgs, P: ToRedisArgs, RK: FromRedisValue>(
                &mut self,
                index_name: K,
                pattern: P,
            ) -> RedisFuture<AsyncIter<'_, RK>> {
                let mut c = redis::cmd("TVS.SCAN");
                c.arg(index_name).cursor_arg(0).arg("MATCH").arg(pattern);
                Box::pin(async move { c.iter_async(self).await })
            }

            #[inline]
            fn tvs_scan_max_dist<K: ToRedisArgs, V: ToRedisArgs, D: ToRedisArgs, RK: FromRedisValue>(
                &mut self,
                index_name: K,
                vector: &V,
                max_dist: D,
            ) -> RedisFuture<AsyncIter<'_, RK>> {
                let mut c = redis::cmd("TVS.SCAN");
                c.arg(index_name)
                    .cursor_arg(0)
                    .arg("VECTOR")
                    .arg(vector)
                    .arg("MAX_DIST")
                    .arg(max_dist);
                Box::pin(async move { c.iter_async(self).await })
            }

            #[inline]
            fn tvs_scan_filter<K: ToRedisArgs, F: ToRedisArgs, RK: FromRedisValue>(
                &mut self,
                index_name: K,
                filter: F,
            ) -> RedisFuture<AsyncIter<'_, RK>> {
                let mut c = redis::cmd("TVS.SCAN");
                c.arg(index_name).cursor_arg(0).arg("FILTER").arg(filter);
                Box::pin(async move { c.iter_async(self).await })
            }

            #[inline]
            fn tvs_scan_full<
                K: ToRedisArgs,
                P: ToRedisArgs,
                V: ToRedisArgs,
                D: ToRedisArgs,
                F: ToRedisArgs,
                RK: FromRedisValue,
            >(
                &mut self,
                index_name: K,
                pattern: Option<P>,
                max_dist: Option<(V, D)>,
                filter: Option<F>,
            ) -> RedisFuture<AsyncIter<'_, RK>> {
                let mut c = redis::cmd("TVS.SCAN").arg(index_name).cursor_arg(0).clone();
                if let Some(p) = pattern {
                    c.arg("MATCH").arg(p);
                }
                if let Some((v, d)) = max_dist {
                    c.arg("VECTOR").arg(v).arg("MAX_DIST").arg(d);
                }
                if let Some(f) = filter {
                    c.arg("FILTER").arg(f);
                }

                Box::pin(async move { c.iter_async(self).await })
            }
        }
    )
}
