package apoc.cache;

import apoc.util.TestUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Stopwatch;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.test.TestGraphDatabaseFactory;

import javax.cache.CacheManager;
import javax.management.MBeanInfo;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static apoc.util.TestUtil.testCall;
import static apoc.util.TestUtil.testResult;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.Assert.*;

/**
 * @author Stefan Armbruster
 */
public class CachingTest {

    private GraphDatabaseService db;

    @Rule
    public Stopwatch stopwatch = new Stopwatch() {
    };

    @Before
    public void setUp() throws Exception {
        db = new TestGraphDatabaseFactory().newImpermanentDatabase();
        TestUtil.registerProcedure(db, Caching.class);
    }

    @After
    public void tearDown() {

        long now = stopwatch.runtime(TimeUnit.MILLISECONDS);
        db.shutdown();
        if (stopwatch.runtime(TimeUnit.MILLISECONDS)-now > 1000) {
            System.out.println("shutdown took longer than 1 sec, probably a transaction is still open");
        };
        CacheManager cacheManager = javax.cache.Caching.getCachingProvider().getCacheManager();
        for (String name: cacheManager.getCacheNames()) {
            cacheManager.destroyCache(name);
        }
    }

    @Test
    public void shouldHaveNoCaches() {
        testResult(db, "call apoc.cache.caches() yield value return value", result -> {
            assertFalse(result.hasNext());
        });
    }

    @Test
    public void shouldCreateAndDestoryCache() {
        // when
        testCall(db, "call apoc.cache.create('cypherResults', {}) yield value return value", stringObjectMap ->
            assertEquals("cypherResults", stringObjectMap.get("value"))
        );

        // then
        testResult(db, "call apoc.cache.caches() yield value return value", result -> {
            List<Map<String, Object>> maps = Iterators.asList(result.columnAs("value"));
            assertEquals(1, maps.size());
            assertEquals("cypherResults", maps.get(0));
        });

        // when
        Iterators.asList(db.execute("call apoc.cache.destroy('cypherResults')"));

        // then: no caches anymore
        testResult(db, "call apoc.cache.caches() yield value return value", result -> {
            assertFalse(result.hasNext());
        });
    }

    @Test
    public void shouldFindStuffInCacheWithExpiry() throws InterruptedException {

        // given
        // create cache with 2 sec expiry
        Iterators.asList(db.execute("call apoc.cache.create('cypherResults', {expires:100, unit:'MILLISECONDS' })"));

        Iterators.asList(db.execute("call apoc.cache.put('cypherResults', 'name', 'JohnDoe')"));

        // when / then
        testResult(db, "call apoc.cache.get('cypherResults', 'name') yield value", result -> {
            List<String> records = Iterators.asList(result.columnAs("value"));
            assertEquals(1, records.size());
            assertEquals("JohnDoe", records.get(0));
        });

        Thread.sleep(100);   // wait for expiry
        testResult(db, "call apoc.cache.get('cypherResults', 'name') yield value", result -> {
            List<Map<String,Object>> records = Iterators.asList(result.columnAs("value"));
            assertEquals(0, records.size());
        });

    }

    @Test
    public void shouldResultBeSameAsRegularCypher() throws Exception {

        // given
        db.execute("CREATE (:Person{name:'John Doe'})");
        Iterators.asList(db.execute("call apoc.cache.create('cypherResults', {expires:100})"));

        // when
        List<Map<String, Object>> resultProcedure = Iterators.asList(db.execute("call apoc.cache.cypher('cypherResults', 'match (n) return n.name as name, count(*) as count', {}) yield value return value.name as name, value.count as count"));
        List<Map<String, Object>> resultDirect = Iterators.asList(db.execute("match (n) return n.name as name, count(*) as count"));

        // then
        assertEquals(resultDirect, resultProcedure);
    }

    @Test
    public void shouldCacheCallBeFaster() throws Exception {

        // given
        db.execute("CREATE (:Person{name:'John Doe'})");
        Iterators.asList(db.execute("call apoc.cache.create('cypherResults', {expires:100})"));
        final String cypher = "match (n) return n.name as name, count(*) as count";
        // burn in: query compilation
        try (Result r = db.execute(cypher)) {
        }
        long start = stopwatch.runtime(TimeUnit.MILLISECONDS);

        // when
        Iterators.asList(db.execute(String.format("call apoc.cache.cypher('cypherResults', '%s', {}) yield value return value.name as name, value.count as count", cypher)));

        long now = stopwatch.runtime(TimeUnit.MILLISECONDS);
        long runtimeFirst = now-start;
        start = now;

        Iterators.asList(db.execute(String.format("call apoc.cache.cypher('cypherResults', '%s', {}) yield value return value.name as name, value.count as count", cypher)));

        now = stopwatch.runtime(TimeUnit.MILLISECONDS);
        long runtimeSecond = now - start;

        // then
        assertThat(runtimeFirst, greaterThan(runtimeSecond));
        assertThat(runtimeSecond, lessThan(20l));
    }

    @Test
    public void shouldExpireCachedResults() throws Exception {

        // given
        Iterators.asList(db.execute("CREATE (:Person{name:'John Doe'})"));
        Iterators.asList(db.execute("call apoc.cache.create('cypherResults', {expires:100, unit:'MILLISECONDS'})"));
        final String cypher = "match (n) return n.name as name, count(*) as count";

        // put into cache
        Iterators.asList(db.execute(String.format("call apoc.cache.cypher('cypherResults', '%s', {}) yield value return value.name as name, value.count as count", cypher)));

        // when: reading from cache
        long start = stopwatch.runtime(TimeUnit.MILLISECONDS);
        Iterators.asList(db.execute(String.format("call apoc.cache.cypher('cypherResults', '%s', {}) yield value return value.name as name, value.count as count", cypher)));
        long now = stopwatch.runtime(TimeUnit.MILLISECONDS);
        long runtimeFirst = now - start;

        // wait for cache expiry
        Thread.sleep(100);
        start = stopwatch.runtime(TimeUnit.MILLISECONDS);
        System.out.println(stopwatch.runtime(TimeUnit.MILLISECONDS));
        Iterators.asList(db.execute(String.format("call apoc.cache.cypher('cypherResults', '%s', {}) yield value return value.name as name, value.count as count", cypher)));
        System.out.println(stopwatch.runtime(TimeUnit.MILLISECONDS));
        now = stopwatch.runtime(TimeUnit.MILLISECONDS);
        long runtimeSecond = now - start;
        System.out.println(runtimeFirst + " -- " + runtimeSecond);

        // then
        assertThat(runtimeFirst, lessThan(runtimeSecond));
        assertThat(runtimeFirst, lessThan(20l));
    }


//        MBeanServer server = ManagementFactory.getPlatformMBeanServer();
//        Set<ObjectName> objectNames = server.queryNames(null, null);
//        for (ObjectName name : objectNames) {
//            MBeanInfo info = server.getMBeanInfo(name);
//
//            System.out.println("bean " + name + " desc: " + info.getDescription());
//        }
//        assertEquals(1, Caching.cache.size());

}