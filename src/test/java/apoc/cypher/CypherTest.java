package apoc.cypher;

import apoc.util.TestUtil;
import apoc.util.Util;
import apoc.util.Utils;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.junit.*;
import org.junit.rules.ExpectedException;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static apoc.util.TestUtil.testCall;
import static apoc.util.TestUtil.testResult;
import static apoc.util.Util.map;
import static java.util.Collections.singletonMap;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

/**
 * @author mh
 * @since 08.05.16
 */
public class CypherTest {

    private static GraphDatabaseService db;

    @Rule
    public ExpectedException thrown= ExpectedException.none();

    @BeforeClass
    public static void setUp() throws Exception {
        db = new TestGraphDatabaseFactory()
                .newImpermanentDatabaseBuilder()
                .setConfig("apoc.import.file.enabled", "true")
                .setConfig("apoc.import.file.use_neo4j_config", "false")
                .setConfig(GraphDatabaseSettings.load_csv_file_url_root,new File("src/test/resources").getAbsolutePath())
                .newGraphDatabase();
        TestUtil.registerProcedure(db, Cypher.class);
        TestUtil.registerProcedure(db, Utils.class);
        TestUtil.registerProcedure(db, CypherFunctions.class);
        TestUtil.registerProcedure(db, Timeboxed.class);
    }

    @AfterClass
    public static void tearDown() {
        db.shutdown();
    }

    @After
    public void clearDB() {
        db.execute("MATCH (n) DETACH DELETE n");
        try (Transaction tx = db.beginTx()) {
            db.schema().getIndexes().forEach(IndexDefinition::drop);
            db.schema().getConstraints().forEach(ConstraintDefinition::drop);
            tx.success();
        }
    }


    @Test
    public void testRun() throws Exception {
        testCall(db, "CALL apoc.cypher.run('RETURN {a} + 7 as b',{a:3})",
                r -> assertEquals(10L, ((Map) r.get("value")).get("b")));
    }
    @Test
    public void testRunNullParams() throws Exception {
        testCall(db, "CALL apoc.cypher.run('RETURN 42 as b',null)",
                r -> assertEquals(42L, ((Map) r.get("value")).get("b")));
    }
    @Test
    public void testRunNoParams() throws Exception {
        testCall(db, "CALL apoc.cypher.run('RETURN 42 as b',{})",
                r -> assertEquals(42L, ((Map) r.get("value")).get("b")));
    }

    @Test
    public void testRunVariable() throws Exception {
        testCall(db, "CALL apoc.cypher.run('RETURN a + 7 as b',{a:3})",
                r -> assertEquals(10L, ((Map) r.get("value")).get("b")));
    }

    @Test
    public void testRunFirstColumnSingle() throws Exception {
        testCall(db, "RETURN apoc.cypher.runFirstColumnSingle('RETURN a + 7 AS b', {a: 3}) AS s",
                r -> assertEquals(10L, (r.get("s"))));
    }

    @Test
    public void testRunFirstColumnMany() throws Exception {
        testCall(db, "RETURN apoc.cypher.runFirstColumnMany('UNWIND range(1,a) as id RETURN id', {a: 3}) AS s",
                r -> assertEquals(Arrays.asList(1L,2L,3L), (r.get("s"))));
    }

    @Test
    public void testRunFirstColumnBugCompiled() throws Exception {
        ResourceIterator<Node> it = db.execute("CREATE (m:Movie  {title:'MovieA'})<-[:ACTED_IN]-(p:Person {name:'PersonA'})-[:ACTED_IN]->(m2:Movie {title:'MovieB'}) RETURN m").columnAs("m");
        Node movie = it.next();
        it.close();
        String query = "WITH {m} AS m MATCH (m)<-[:ACTED_IN]-(:Person)-[:ACTED_IN]->(rec:Movie) RETURN rec LIMIT 10";
        System.out.println(db.execute("EXPLAIN "+query).getExecutionPlanDescription().toString());
        ResourceIterator<Node> rec = db.execute(query, map("m",movie)).columnAs("rec");
        assertEquals(1, rec.stream().count());
    }
    @Test
    public void testRunFirstColumnBugDirection() throws Exception {
        db.execute("CREATE (m:Movie  {title:'MovieA'})<-[:ACTED_IN]-(p:Person {name:'PersonA'})-[:ACTED_IN]->(m2:Movie {title:'MovieB'})").close();
        String query = "MATCH (m:Movie {title:'MovieA'}) RETURN apoc.cypher.runFirstColumn('WITH {m} AS m MATCH (m)<-[:ACTED_IN]-(:Person)-[:ACTED_IN]->(rec:Movie) RETURN rec LIMIT 10', {m:m}, true) as rec";
        testCall(db, query,
                r -> assertEquals("MovieB", ((Node)((List)r.get("rec")).get(0)).getProperty("title")));
    }

    @Test
    public void testRunFirstColumnMultipleValues() throws Exception {
        List expected = Arrays.asList(1L, 2L, 3L);
        testCall(db, "RETURN apoc.cypher.runFirstColumn('UNWIND [1, 2, 3] AS e RETURN e', {}, true) AS arr",
                r -> assertEquals(expected, r.get("arr")));
    }

    @Test
    public void testParallel() throws Exception {
        int size = 10_000;
        testResult(db, "CALL apoc.cypher.parallel2('UNWIND range(0,9) as b RETURN b',{a:range(1,{size})},'a')", map("size", size),
                r -> assertEquals( size * 10,Iterators.count(r) ));
    }
    @Test
    public void testSingular() throws Exception {
        int size = 10_000;
        testResult(db, "CALL apoc.cypher.run('UNWIND a as row UNWIND range(0,9) as b RETURN b',{a:range(1,{size})})", map("size", size),
                r -> assertEquals( size * 10,Iterators.count(r) ));
    }
    @Test
    public void testMapParallel() throws Exception {
        int size = 10_000;
        testResult(db, "CALL apoc.cypher.mapParallel('UNWIND range(0,9) as b RETURN b',{},range(1,{size}))", map("size", size),
                r -> assertEquals( size * 10,Iterators.count(r) ));
    }
    @Test
    public void testMapParallel2() throws Exception {
        int size = 10_000;
        testResult(db, "CALL apoc.cypher.mapParallel2('UNWIND range(0,9) as b RETURN b',{},range(1,{size}),10)", map("size", size),
                r -> assertEquals( size * 10,Iterators.count(r) ));
    }
    @Test
    public void testParallel2() throws Exception {
        int size = 10_0000;
        List<Long> list = new ArrayList<>(size);
        for (int i = 0; i < size; i++) list.add(3L);
        testCall(db, "CALL apoc.cypher.parallel2('RETURN a + 7 as b',{a:{list}},'a') YIELD value RETURN sum(value.b) as b", map("list", list),
                r -> {
                    assertEquals( size * 10L, r.get("b") );
                });
    }

    private long toLong(Object value) {
    	return Util.toLong(value);
    }

    @Test
    public void testRunMany() throws Exception {
        testResult(db, "CALL apoc.cypher.runMany('CREATE (n:Node {name:{name}});\nMATCH (n {name:{name}}) CREATE (n)-[:X {name:{name2}}]->(n);',{params})",map("params",map("name","John","name2","Doe")),
                r -> {
                    Map<String, Object> row = r.next();
                    assertEquals(-1L, row.get("row"));
                    Map result = (Map) row.get("result");
                    assertEquals(1L, toLong(result.get("nodesCreated")));
                    assertEquals(1L, toLong(result.get("labelsAdded")));
                    assertEquals(1L, toLong(result.get("propertiesSet")));
                    row = r.next();
                    result = (Map) row.get("result");
                    assertEquals(-1L, row.get("row"));
                    assertEquals(1L, toLong(result.get("relationshipsCreated")));
                    assertEquals(1L, toLong(result.get("propertiesSet")));
                    assertEquals(false, r.hasNext());
                });
    }
    @Test
    public void testRunFile() throws Exception {
        testResult(db, "CALL apoc.cypher.runFile('src/test/resources/create_delete.cypher')",
                r -> {
                    Map<String, Object> row = r.next();
                    assertEquals(-1L, row.get("row"));
                    Map result = (Map) row.get("result");
                    assertEquals(1L, toLong(result.get("nodesCreated")));
                    assertEquals(1L, toLong(result.get("labelsAdded")));
                    assertEquals(1L, toLong(result.get("propertiesSet")));
                    row = r.next();
                    result = (Map) row.get("result");
                    assertEquals(-1L, row.get("row"));
                    assertEquals(1L, toLong(result.get("nodesDeleted")));
                    assertEquals(false, r.hasNext());
                });
    }
    @Test
    public void testRunWithPeriodic() throws Exception {
        testResult(db, "CALL apoc.cypher.runFile('src/test/resources/periodic.cypher')",
                r -> {
                    Map<String, Object> row = r.next();
                    assertEquals(-1L, row.get("row"));
                    Map result = (Map) row.get("result");
                    assertEquals(1L, toLong(result.get("nodesCreated")));
                    assertEquals(1L, toLong(result.get("labelsAdded")));
                    assertEquals(2L, toLong(result.get("propertiesSet")));
                    assertEquals(false, r.hasNext());
                });
    }

    @Test
    public void testRunFileWithSchema() throws Exception {
        testResult(db, "CALL apoc.cypher.runFile('src/test/resources/schema_create.cypher')",
                r -> {
                    Map<String, Object> row = r.next();
                    assertEquals(-1L, row.get("row"));
                    Map result = (Map) row.get("result");
                    assertEquals(0L, toLong(result.get("indexesAdded")));
                    assertEquals(1L, toLong(result.get("nodesCreated")));
                    assertEquals(1L, toLong(result.get("propertiesSet")));
                    assertEquals(false, r.hasNext());
                });
    }
    @Test
    public void testRunFileWithResults() throws Exception {
        testResult(db, "CALL apoc.cypher.runFile('src/test/resources/create.cypher')",
                r -> {
                    Map<String, Object> row = r.next();
                    assertEquals(row.get("row"),((Map)row.get("result")).get("id"));
                    row = r.next();
                    assertEquals(row.get("row"),((Map)row.get("result")).get("id"));
                    row = r.next();
                    assertEquals(row.get("row"),((Map)row.get("result")).get("id"));
                    row = r.next();
                    assertEquals(-1L, row.get("row"));
                    Map result = (Map) row.get("result");
                    assertEquals(3L, toLong(result.get("nodesCreated")));
                    assertEquals(3L, toLong(result.get("labelsAdded")));
                    assertEquals(3L, toLong(result.get("propertiesSet")));

                    row = r.next();
                    result = (Map) row.get("result");
                    assertEquals(-1L, row.get("row"));
                    assertEquals(3L, toLong(result.get("nodesDeleted")));
                    assertEquals(false, r.hasNext());
                });
    }
    @Test
    public void testRunFileWithParameters() throws Exception {
        testResult(db, "CALL apoc.cypher.runFile('src/test/resources/parameterized.cypher', {statistics:false,parameters:{foo:123,bar:'baz'}})",
                r -> {
                    assertTrue("first row",r.hasNext());
                    Map<String,Object> result = (Map<String,Object>)r.next().get("result");
                    assertEquals(result.toString(), 1, result.size());
                    assertThat( result, hasEntry("one", 123L));
                    assertTrue("second row",r.hasNext());
                    result = (Map<String,Object>)r.next().get("result");
                    assertEquals(result.toString(), 1, result.size());
                    assertThat(result, hasEntry("two", "baz"));
                    assertTrue("third row",r.hasNext());
                    result = (Map<String,Object>)r.next().get("result");
                    assertEquals(result.toString(), 2, result.size());
                    assertThat(result, hasEntry("foo", 123L));
                    assertThat(result, hasEntry("bar", "baz"));
                    assertFalse("fourth row",r.hasNext());
                });
    }

    @Test
    public void testRunFilesMultiple() throws Exception {
        testResult(db, "CALL apoc.cypher.runFiles(['src/test/resources/create.cypher', 'src/test/resources/create_delete.cypher'])",
                r -> {
                    Map<String, Object> row = r.next();
                    assertEquals(row.get("row"),((Map)row.get("result")).get("id"));
                    row = r.next();
                    assertEquals(row.get("row"),((Map)row.get("result")).get("id"));
                    row = r.next();
                    assertEquals(row.get("row"),((Map)row.get("result")).get("id"));
                    row = r.next();
                    assertEquals(-1L, row.get("row"));
                    Map result = (Map) row.get("result");
                    assertEquals(3L, toLong(result.get("nodesCreated")));
                    assertEquals(3L, toLong(result.get("labelsAdded")));
                    assertEquals(3L, toLong(result.get("propertiesSet")));
                    row = r.next();
                    result = (Map) row.get("result");
                    assertEquals(3L, toLong(result.get("nodesDeleted")));
                    row = r.next();
                    result = (Map) row.get("result");
                    assertEquals(-1L, row.get("row"));
                    assertEquals(1L, toLong(result.get("nodesCreated")));
                    assertEquals(1L, toLong(result.get("labelsAdded")));
                    assertEquals(1L, toLong(result.get("propertiesSet")));
                    row = r.next();
                    result = (Map) row.get("result");
                    assertEquals(-1L, row.get("row"));
                    assertEquals(1L, toLong(result.get("nodesDeleted")));
                    assertEquals(false, r.hasNext());
                });
    }

    @Test
    @Ignore
    public void testSchemaRunFile() throws Exception {
        testResult(db, "CALL apoc.cypher.runSchemaFile('src/test/resources/schema.cypher')",
                r -> {
                    Map<String, Object> row = r.next();
                    Map result = (Map) row.get("result");
                    assertEquals(1L, toLong(result.get("indexesAdded")));
                });
    }

    @Test
    @Ignore
    public void testSchemaRunFiles() throws Exception {
        testResult(db, "CALL apoc.cypher.runSchemaFiles(['src/test/resources/constraints.cypher', 'src/test/resources/drop_constraints.cypher', 'src/test/resources/index.cypher'])",
                r -> {
                    Map<String, Object> row = r.next();
                    Map result = (Map) row.get("result");
                    assertEquals(1L, toLong(result.get("constraintsAdded")));
                    row = r.next();
                    result = (Map) row.get("result");
                    assertEquals(1L, toLong(result.get("constraintsRemoved")));
                    row = r.next();
                    result = (Map) row.get("result");
                    assertEquals(1L, toLong(result.get("indexesAdded")));

                });
    }

    @Test
    @Ignore
    public void testSchemaRunMixedSchemaAndDataFile() throws Exception {
        testResult(db, "CALL apoc.cypher.runSchemaFile('src/test/resources/schema_create.cypher')",
                r -> {
                    Map<String, Object> row = r.next();
                    Map result = (Map) row.get("result");
                    assertEquals(1L, toLong(result.get("indexesAdded")));
                });
    }

    @Test(timeout=9000)
    public void testWithTimeout() {
        Result result = db.execute("CALL apoc.cypher.runTimeboxed('CALL apoc.util.sleep(10000)', null, $timeout)", singletonMap("timeout", 100));
        assertFalse( result.hasNext());
    }

    @Test(timeout=9000)
    public void testWithTimeoutAndStatus() {
        Map<String, Object> result = Iterators.single(db.execute("CALL apoc.cypher.runTimeboxed('CALL apoc.util.sleep(10000)', null, $timeout, true)", singletonMap("timeout", 100)));

        Map<String, Object> value = (Map<String, Object>) result.get("value");
        assertThat(value,
                allOf(
                        hasEntry("status", "TERMINATED"),
                        hasEntry("error", null)
                ));
    }

    @Test
    public void shouldTimeboxedReturnAllResultsSoFar() {
        db.execute(Util.readResourceFile("movies.cypher"));
//        System.out.println("movies imported");

        long start = System.currentTimeMillis();
        try (Transaction tx = db.beginTx()) {
            Result result = db.execute("CALL apoc.cypher.runTimeboxed('match(n) -[*]-(m) return id(n),id(m)', {}, 1000) YIELD value RETURN value");
            assertTrue(Iterators.count(result)>0);
            tx.success();
        }
        long duration= System.currentTimeMillis() - start;
        assertThat("test runs in less than 1500 millis", duration, Matchers.lessThan(1500l));
    }

    @Test(timeout=9000)
    public void shouldTooLongTimeboxBeNotHarmful() {
        Result result = db.execute("CALL apoc.cypher.runTimeboxed('CALL apoc.util.sleep(10)', null, $timeout)", singletonMap("timeout", 10000));
        assertFalse(result.hasNext());

        result = db.execute("CALL apoc.cypher.runTimeboxed('CALL apoc.util.sleep(10)', null, $timeout, true)", singletonMap("timeout", 10000));

        Map<String, Object> single = Iterators.single(result);
        Map<String, Object> value = (Map<String, Object>) single.get("value");
        assertThat(value,
                allOf(
                        hasEntry("status", "COMPLETE"),
                        hasEntry("error", null)
                ));
    }

    @Test
    public void testInvalidCypherInTimebox() {
        Result result = db.execute("CALL apoc.cypher.runTimeboxed('this is not cypher', null, $timeout)", singletonMap("timeout", 1000));
        assertFalse(result.hasNext());

        result = db.execute("CALL apoc.cypher.runTimeboxed('this is not cypher', null, $timeout, true)", singletonMap("timeout", 1000));
        Map<String, Object> single = Iterators.single(result);

        Map<String, Object> value = (Map<String, Object>) single.get("value");
        assertThat(value,
                allOf(
                        hasEntry("status", "ERROR"),
                        hasEntry( equalTo("error"), stringValue(containsString("Invalid input")))
                ));
    }

    private static<T> Matcher<Object> stringValue(final Matcher<T> subMatcher) {
        return new BaseMatcher<Object>() {
            @Override
            public boolean matches(Object item) {
                return subMatcher.matches(String.class.cast(item));
            }

            @Override
            public void describeTo(Description description) {
                description.appendDescriptionOf(subMatcher);
            }

            @Override
            public void describeMismatch(Object item, Description description) {
                subMatcher.describeMismatch(item, description);
            }
        };
    }

    @Test
    public void testSimpleWhenIfCondition() throws Exception {
        testCall(db, "CALL apoc.when(true, 'RETURN 7 as b')",
                r -> assertEquals(7L, ((Map) r.get("value")).get("b")));
    }

    @Test
    public void testSimpleWhenElseCondition() throws Exception {
        testCall(db, "CALL apoc.when(false, 'RETURN 7 as b') YIELD value RETURN value",
                r -> assertEquals(null, ((Map) r.get("value")).get("b")));
    }

    @Test
    public void testWhenIfCondition() throws Exception {
        testCall(db, "CALL apoc.when(true, 'RETURN {a} + 7 as b', 'RETURN {a} as b',{a:3})",
                r -> assertEquals(10L, ((Map) r.get("value")).get("b")));
    }

    @Test
    public void testWhenElseCondition() throws Exception {
        testCall(db, "CALL apoc.when(false, 'RETURN {a} + 7 as b', 'RETURN {a} as b',{a:3})",
                r -> assertEquals(3L, ((Map) r.get("value")).get("b")));
    }

    @Test
    public void testDoWhenIfCondition() throws Exception {
        testCall(db, "CALL apoc.do.when(true, 'CREATE (a:Node{name:\"A\"}) RETURN a.name as aName', 'CREATE (b:Node{name:\"B\"}) RETURN b.name as bName',{})",
                r -> {
                    assertEquals("A", ((Map) r.get("value")).get("aName"));
                    assertEquals(null, ((Map) r.get("value")).get("bName"));
                });
    }

    @Test
    public void testDoWhenElseCondition() throws Exception {
        testCall(db, "CALL apoc.do.when(false, 'CREATE (a:Node{name:\"A\"}) RETURN a.name as aName', 'CREATE (b:Node{name:\"B\"}) RETURN b.name as bName',{})",
                r -> {
                    assertEquals("B", ((Map) r.get("value")).get("bName"));
                    assertEquals(null, ((Map) r.get("value")).get("aName"));
                });
    }

    @Test
    public void testCase() throws Exception {
        testCall(db, "CALL apoc.case([false, 'RETURN {a} + 7 as b', false, 'RETURN {a} as b', true, 'RETURN {a} + 4 as b', false, 'RETURN {a} + 1 as b'], 'RETURN {a} + 10 as b', {a:3})",
                r -> assertEquals(7L, ((Map) r.get("value")).get("b")));
    }

    @Test
    public void testCaseElseCondition() throws Exception {
        testCall(db, "CALL apoc.case([false, 'RETURN {a} + 7 as b', false, 'RETURN {a} as b', false, 'RETURN {a} + 4 as b'], 'RETURN {a} + 10 as b', {a:3})",
                r -> assertEquals(13L, ((Map) r.get("value")).get("b")));
    }

    @Test
    public void testSimpleCase() throws Exception {
        testCall(db, "CALL apoc.case([false, 'RETURN 3 + 7 as b', false, 'RETURN 3 as b', true, 'RETURN 3 + 4 as b'])",
                r -> assertEquals(7L, ((Map) r.get("value")).get("b")));
    }

    @Test
    public void testSimpleCaseElseCondition() throws Exception {
        testCall(db, "CALL apoc.case([false, 'RETURN 3 + 7 as b', false, 'RETURN 3 as b', false, 'RETURN 3 + 4 as b'], 'RETURN 3 + 10 as b')",
                r -> assertEquals(13L, ((Map) r.get("value")).get("b")));
    }

    @Test
    public void testCaseDo() throws Exception {
        testCall(db, "CALL apoc.do.case([false, 'CREATE (a:Node{name:\"A\"}) RETURN a.name as aName', true, 'CREATE (b:Node{name:\"B\"}) RETURN b.name as bName'], 'CREATE (c:Node{name:\"C\"}) RETURN c.name as cName',{})",
                r -> {
                    assertEquals(null, ((Map) r.get("value")).get("aName"));
                    assertEquals("B", ((Map) r.get("value")).get("bName"));
                    assertEquals(null, ((Map) r.get("value")).get("cName"));
                });
    }

    @Test
    public void testCaseDoElseCondition() throws Exception {
        testCall(db, "CALL apoc.do.case([false, 'CREATE (a:Node{name:\"A\"}) RETURN a.name as aName', false, 'CREATE (b:Node{name:\"B\"}) RETURN b.name as bName'], 'CREATE (c:Node{name:\"C\"}) RETURN c.name as cName',{})",
                r -> {
                    assertEquals(null, ((Map) r.get("value")).get("aName"));
                    assertEquals(null, ((Map) r.get("value")).get("bName"));
                    assertEquals("C", ((Map) r.get("value")).get("cName"));
                });
    }
}
