package apoc.export.csv;

import apoc.export.xls.ExportXls;
import apoc.graph.Graphs;
import apoc.util.TestUtil;
import org.apache.poi.openxml4j.exceptions.InvalidFormatException;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

import static apoc.util.MapUtil.map;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ExportXlsTest {

    private static GraphDatabaseService db;
    private static File directory = new File("target/import");

    static { //noinspection ResultOfMethodCallIgnored
        directory.mkdirs();
    }

    @BeforeClass
    public static void setUp() throws Exception {
        db = new TestGraphDatabaseFactory()
                .newImpermanentDatabaseBuilder()
                .setConfig(GraphDatabaseSettings.load_csv_file_url_root, directory.getAbsolutePath())
                .setConfig("apoc.export.file.enabled", "true")
                .newGraphDatabase();
        TestUtil.registerProcedure(db, ExportXls.class, Graphs.class);
        db.execute("CREATE (f:User1:User {name:'foo',age:42,male:true,kids:['a','b','c'],location:point({longitude: 11.8064153, latitude: 48.1716114}),dob:date({ year:1984, month:10, day:11 }), created: datetime()})-[:KNOWS]->(b:User {name:'bar',age:42}),(c:User {age:12})").close();
        db.execute("CREATE (f:Address1:Address {name:'Andrea', city: 'Milano', street:'Via Garibaldi, 7'})-[:NEXT_DELIVERY]->(a:Address {name: 'Bar Sport'}), (b:Address {street: 'via Benni'})").close();
    }

    @AfterClass
    public static void tearDown() {
        db.shutdown();
    }

    @Test
    public void testExportAllXls() throws Exception {
        File output = new File(directory, "all.xlsx");
        TestUtil.testCall(db, "CALL apoc.export.xls.all({file},null)", map("file", output.getAbsolutePath()),
                (r) -> assertResults(output, r, "database"));

        assertExcelFileForGraph(output);
    }

    @Test
    public void testExportGraphXls() throws Exception {
        File output = new File(directory, "graph.xlsx");
        TestUtil.testCall(db, "CALL apoc.graph.fromDB('test',{}) yield graph " +
                        "CALL apoc.export.xls.graph(graph, {file},null) " +
                        "YIELD nodes, relationships, properties, file, source,format, time " +
                        "RETURN *", map("file", output.getAbsolutePath()),
                (r) -> assertResults(output, r, "graph"));
        assertExcelFileForGraph(output);
    }

    @Test
    public void testExportQueryXls() throws Exception {
        File output = new File(directory, "query.xlsx");
        String query = "MATCH (u:User) return u.age, u.name, u.male, u.kids, labels(u)";
        TestUtil.testCall(db, "CALL apoc.export.xls.query({query},{file},null)", map("file", output.getAbsolutePath(),"query",query),
                (r) -> {
                    assertTrue("Should get statement",r.get("source").toString().contains("statement: cols(5)"));
                    assertEquals(output.getAbsolutePath(), r.get("file"));
                    assertEquals("xls", r.get("format"));

                });
        assertExcelFileForQuery(output);
    }


    private void assertResults(File output, Map<String, Object> r, final String source) {
        assertEquals(8L, r.get("nodes")); // we're exporting nodes with multiple label multiple times
        assertEquals(2L, r.get("relationships"));
        assertEquals(25L, r.get("properties"));
        assertEquals(source + ": nodes(6), rels(2)", r.get("source"));
        assertEquals(output.getAbsolutePath(), r.get("file"));
        assertEquals("xls", r.get("format"));
        assertTrue("Should get time greater than 0", ((long) r.get("time")) >= 0);
    }

    private void assertExcelFileForGraph(File output) {
        try (InputStream inp = new FileInputStream(output); Transaction tx = db.beginTx()) {
            Workbook wb = WorkbookFactory.create(inp);

            int numberOfSheets = wb.getNumberOfSheets();
            assertEquals(Iterables.count(db.getAllLabels()) + Iterables.count(db.getAllRelationshipTypes()), numberOfSheets);

            for (Label label: db.getAllLabels()) {
                long numberOfNodes = Iterators.count(db.findNodes(label));
                Sheet sheet = wb.getSheet(label.name());
                assertEquals(numberOfNodes, sheet.getLastRowNum());
            }
            tx.success();
        } catch (IOException|InvalidFormatException e) {
            throw new RuntimeException(e);
        }
    }

    private void assertExcelFileForQuery(File output) {
        try (InputStream inp = new FileInputStream(output)) {
            Workbook wb = WorkbookFactory.create(inp);

            int numberOfSheets = wb.getNumberOfSheets();
            assertEquals(1, numberOfSheets);

        } catch (IOException|InvalidFormatException e) {
            throw new RuntimeException(e);
        }
    }
}
