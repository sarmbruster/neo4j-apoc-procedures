package apoc.cache;

import apoc.algo.PathFinding;
import apoc.util.TestUtil;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Stopwatch;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.util.List;
import java.util.Map;
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
        db.shutdown();
    }

    @Test
    public void shouldResultBeSameAsRegularCypher() throws Exception {

        // given
        db.execute("CREATE (:Person{name:'John Doe'})");

        // when
        List<Map<String, Object>> resultProcedure = Iterators.asList(db.execute("call apoc.cache.cypherResults('match (n) return n.name as name, count(*) as count', {}, 10000) yield value return value.name as name, value.count as count"));
        List<Map<String, Object>> resultDirect = Iterators.asList(db.execute("match (n) return n.name as name, count(*) as count"));

        // then
        assertEquals(resultDirect, resultProcedure);
    }

    @Test
    public void shouldCacheCallBeFaster() throws Exception {

        // given
        db.execute("CREATE (:Person{name:'John Doe'})");
        final String cypher = "match (n) return n.name as name, count(*) as count";
        // burn in: query compilation
        try (Result r = db.execute(cypher)) {
        }
        long start = stopwatch.runtime(TimeUnit.MILLISECONDS);

        // when
        Iterators.asList(db.execute(String.format("call apoc.cache.cypherResults('%s', {}, 100000) yield value return value.name as name, value.count as count", cypher)));

        long now = stopwatch.runtime(TimeUnit.MILLISECONDS);
        long runtimeFirst = now-start;
        start = now;

        Iterators.asList(db.execute(String.format("call apoc.cache.cypherResults('%s', {}, 100000) yield value return value.name as name, value.count as count", cypher)));

        now = stopwatch.runtime(TimeUnit.MILLISECONDS);
        long runtimeSecond = now - start;

        // then
        assertThat(runtimeFirst, greaterThan(runtimeSecond));
        assertThat(runtimeSecond, lessThan(20l));
    }

    @Test
    public void shouldExpireCachedResults() throws Exception {

        // given
        db.execute("CREATE (:Person{name:'John Doe'})");
        final String cypher = "match (n) return n.name as name, count(*) as count";

        // when
        Iterators.asList(db.execute(String.format("call apoc.cache.cypherResults('%s', {}, 100) yield value return value.name as name, value.count as count", cypher)));
        long start = stopwatch.runtime(TimeUnit.MILLISECONDS);
        assertEquals(1, Caching.cache.size());

        Iterators.asList(db.execute(String.format("call apoc.cache.cypherResults('%s', {}, 100) yield value return value.name as name, value.count as count", cypher)));
        long now = stopwatch.runtime(TimeUnit.MILLISECONDS);
        long runtimeFirst = now - start;
        assertEquals(1, Caching.cache.size());

        Thread.sleep(100);
        assertEquals(0, Caching.cache.size());
        start = stopwatch.runtime(TimeUnit.MILLISECONDS);
        Iterators.asList(db.execute(String.format("call apoc.cache.cypherResults('%s', {}, 100) yield value return value.name as name, value.count as count", cypher)));
        now = stopwatch.runtime(TimeUnit.MILLISECONDS);
        long runtimeSecond = now - start;

        // then
        assertThat(runtimeFirst, lessThan(runtimeSecond));
        assertThat(runtimeFirst, lessThan(20l));
    }


}