package apoc.cache;

import apoc.Description;
import apoc.result.MapResult;
import apoc.result.ObjectResult;
import apoc.result.StringResult;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.helpers.collection.Pair;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import javax.cache.Cache;
import javax.cache.CacheException;
import javax.cache.CacheManager;
import javax.cache.annotation.CacheResult;
import javax.cache.configuration.Configuration;
import javax.cache.configuration.MutableConfiguration;
import javax.cache.expiry.Duration;
import javax.cache.expiry.ModifiedExpiryPolicy;
import javax.cache.spi.CachingProvider;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static org.neo4j.helpers.collection.MapUtil.map;

/**
 * @author Stefan Armbruster
 */
public class Caching {

    public static final String CACHE_NAME_CYPHER_RESULTS = "cypherResults";

    /*
     * holds cypher + params as key -> maps to a materialized result
     */
//    public static final TTLMap<Pair<String,Map<String,Object>>, List<Map<String,Object>>> cache = new TTLMapImpl<>();

    /*
                    MutableConfiguration config =
                 new MutableConfiguration()
                      .setStoreByValue(false)
                      .setTypes(Pair.class, List.class)
//                      .setExpiryPolicyFactory(AccessedExpiryPolicy.factoryOf(ONE_HOUR))
                      .setStatisticsEnabled(true)
                      .setManagementEnabled(true);
                CACHE_MANAGER.createCache(CACHE_NAME_CYPHER_RESULTS, config);
     */

    @Context
    public GraphDatabaseService graphDatabaseService;

    // cache management
    @Procedure
    @Description("call apoc.cache.caches() yield value - list all existing caches'")
    public Stream<StringResult> caches() {
        return StreamSupport.stream(javax.cache.Caching.getCachingProvider().getCacheManager().getCacheNames().spliterator(), false).map( s -> new StringResult(s));
    }

/*
    @Procedure
    @Description("call apoc.cache.providers() yield value - info about cache providers")
    public Stream<MapResult> providers() {
        return StreamSupport.stream(javax.cache.Caching.getCachingProviders().spliterator(), false).map(
                cachingProvider -> new MapResult(map("URI", cachingProvider.getDefaultURI(), "name", cachingProvider.getClass().getName()))
        );
    }
*/

    @Procedure
    @Description("call apoc.cache.create('cypherResults', {expires: 5}) - create a named cache with (optional) expiry in seconds")
    public Stream<StringResult> create(@Name("cacheName") String cacheName, @Name("properties") Map<String,Object> properties) {
        long expiryValue = 0; // defaulting to eternal
        TimeUnit expiryUnit = TimeUnit.SECONDS;

        if (properties!=null) {
            Object expires = properties.get("expires");
            if (expires!=null) {
                expiryValue = (long) expires;
            }
            String unit = (String) properties.get("unit");
            if (unit!=null) {
                expiryUnit = TimeUnit.valueOf(unit);
            }

        }

        CacheManager cacheManager = javax.cache.Caching.getCachingProvider().getCacheManager();

        Configuration<Object, Object> configuration = new MutableConfiguration<>()
        .setStoreByValue(false)
        //.setTypes(Pair.class, List.class)
        .setExpiryPolicyFactory(ModifiedExpiryPolicy.factoryOf(new Duration(expiryUnit, expiryValue)))
        .setStatisticsEnabled(true)
        .setManagementEnabled(true);
        Cache cache = cacheManager.createCache(cacheName, configuration);

        return Stream.of(new StringResult(cache.getName()));
    }

    @Procedure
    @Description("call apoc.cache.destroy('cypherResults') - destroy a cache" )
    public void destroy(@Name("cacheName") String cacheName) {
        CacheManager cacheManager = javax.cache.Caching.getCachingProvider().getCacheManager();
        cacheManager.destroyCache(cacheName);
    }

    @Procedure
    @Description("call apoc.cache.clear('cypherResults') - removes all elements from a cache" )
    public void clear(@Name("cacheName") String cacheName) {
        Cache cache = getCacheOrException(cacheName);
        cache.clear();
    }

    @Procedure
    @Description("call apoc.cache.put('mycache', 'key', 'value') - add stuff to cache")
    public void put(@Name("cacheName") String cacheName, @Name("key") Object key, @Name("value") Object value) {
        Cache cache = getCacheOrException(cacheName);
        cache.put(key, value);
    }

    @Procedure
    @Description("call apoc.cache.get('mycache', 'key') - get stuff from cache")
    public Stream<ObjectResult> get(@Name("cacheName") String cacheName, @Name("key") Object key) {
        Cache cache = getCacheOrException(cacheName);
        Object value = cache.get(key);
        Stream<ObjectResult> result = value == null ? Stream.of() : Stream.of(new ObjectResult(value));
        return result;
    }

    @Procedure
    @Description("call apoc.cache.cypher('cypherResults', 'match (n) return n.name as name, count(*) as count', {}) yield value return value.name as name, value.count as count")
    public Stream<MapResult> cypher(@Name("cacheName") String cacheName,
                                    @Name("cypher") String cypher,
                                    @Name("params") Map<String, Object> params) {
        Cache cache = getCacheOrException(cacheName);

        Pair<String, Map<String,Object>> key = Pair.of(cypher, params);

        List<Map<String, Object>> result = (List<Map<String, Object>>) cache.get(key);
        if (result==null) {
            result = Iterators.asList(graphDatabaseService.execute(cypher, params));
            cache.put(key, result);
        }
        return result.stream().map(stringObjectMap -> new MapResult(stringObjectMap));
    }

    private Cache getCacheOrException(String cacheName) {
        Cache<Object, Object> cache = javax.cache.Caching.getCache(cacheName, Object.class, Object.class);
        if (cache==null) {
            throw new CacheException(String.format("no cache with name %s found. Caches need to be created first, see apoc.cache.create", cacheName));
        }
        return cache;
    }

}
