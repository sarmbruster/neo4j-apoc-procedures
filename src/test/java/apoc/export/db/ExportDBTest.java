package apoc.export.db;

import apoc.custom.CypherProcedures;
import apoc.graph.Graphs;
import apoc.util.TestUtil;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;

import static apoc.ApocConfig.APOC_EXPORT_FILE_ENABLED;
import static apoc.ApocConfig.apocConfig;

public class ExportDBTest {

    @Rule
    public TemporaryFolder STORE_DIR = new TemporaryFolder();

    private GraphDatabaseService db;
    private DatabaseManagementService databaseManagementService;

    @Before
    public void setUp() throws Exception {
        databaseManagementService = new TestDatabaseManagementServiceBuilder(STORE_DIR.getRoot()).build();
        db = databaseManagementService.database(GraphDatabaseSettings.DEFAULT_DATABASE_NAME);
        TestUtil.registerProcedure(db, CypherProcedures.class);
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


//    @Rule
//    public TestName testName = new TestName();

//    private static final String OPTIMIZED = "Optimized";
//    private static final String ODD = "OddDataset";


    @Test
    public void testExportAllCypherResults() {
        TestUtil.testCall(db, "CALL apoc.export.db.all('copydb', {batchImport: true})", (r) -> {
//            assertResults(null, r, "database");
//            assertEquals(EXPECTED_NEO4J_SHELL, r.get("cypherStatements"));
        });
    }


}
