package apoc.export.db;

import apoc.graph.Graphs;
import apoc.util.TestUtil;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import static apoc.ApocConfig.APOC_EXPORT_FILE_ENABLED;
import static apoc.ApocConfig.apocConfig;

public class ExportDBTest {

//    private static final Map<String, Object> exportConfig = map("useOptimizations", map("type", "none"), "separateFiles", true, "format", "neo4j-admin");

    @Rule
    public DbmsRule db = new ImpermanentDbmsRule();
//            .withSetting(GraphDatabaseSettings.load_csv_file_url_root, directory.toPath().toAbsolutePath());

    @Rule
    public TestName testName = new TestName();

//    private static final String OPTIMIZED = "Optimized";
//    private static final String ODD = "OddDataset";

    @Before
    public void setUp() throws Exception {
        apocConfig().setProperty(APOC_EXPORT_FILE_ENABLED, true);
        TestUtil.registerProcedure(db, ExportDb.class, Graphs.class);
        db.executeTransactionally("CREATE INDEX ON :Bar(first_name, last_name)");
        db.executeTransactionally("CREATE INDEX ON :Foo(name)");
        db.executeTransactionally("CREATE CONSTRAINT ON (b:Bar) ASSERT b.name IS UNIQUE");
//        if (testName.getMethodName().endsWith(OPTIMIZED)) {
//            db.executeTransactionally("CREATE (f:Foo {name:'foo', born:date('2018-10-31')})-[:KNOWS {since:2016}]->(b:Bar {name:'bar',age:42}),(c:Bar:Person {age:12}),(d:Bar {age:12})," +
//                    " (t:Foo {name:'foo2', born:date('2017-09-29')})-[:KNOWS {since:2015}]->(e:Bar {name:'bar2',age:44}),({age:99})");
//        } else if(testName.getMethodName().endsWith(ODD)) {
//            db.executeTransactionally("CREATE (f:Foo {name:'foo', born:date('2018-10-31')})," +
//                    "(t:Foo {name:'foo2', born:date('2017-09-29')})," +
//                    "(g:Foo {name:'foo3', born:date('2016-03-12')})," +
//                    "(b:Bar {name:'bar',age:42})," +
//                    "(c:Bar {age:12})," +
//                    "(d:Bar {age:4})," +
//                    "(e:Bar {name:'bar2',age:44})," +
//                    "(f)-[:KNOWS {since:2016}]->(b)");
//        } else {
            db.executeTransactionally("CREATE (f:Foo {name:'foo', born:date('2018-10-31')})-[:KNOWS {since:2016}]->(b:Bar {name:'bar',age:42}),(c:Bar {age:12})");
//        }
    }

    @Test
    public void testExportAllCypherResults() {
        TestUtil.testCall(db, "CALL apoc.export.db.all('copydb', {batchImport: true})", (r) -> {
//            assertResults(null, r, "database");
//            assertEquals(EXPECTED_NEO4J_SHELL, r.get("cypherStatements"));
        });
    }


}
