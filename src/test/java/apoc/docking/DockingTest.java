package apoc.docking;

import apoc.create.Create;
import apoc.util.TestUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.*;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.util.Map;

import static apoc.util.TestUtil.testCall;
import static apoc.util.TestUtil.testResult;
import static org.junit.Assert.*;

public class DockingTest {

    private GraphDatabaseService db;
    public static final Label PERSON = Label.label("Person");

    @Before
    public void setUp() throws Exception {
        db = new TestGraphDatabaseFactory().newImpermanentDatabase();
        TestUtil.registerProcedure(db,Docking.class);
    }

    @After
    public void tearDown() {
        db.shutdown();
    }

    @Test
    public void shouldAddingDockingNodesWork() {
        testCall(db, "create (c:Company{name:'Neo4j'}), (p:Person{name:'Stefan'}) with c,p " +
                "call apoc.docking.connect(c, p, '<WORKS_FOR') yield rel return rel",
                row -> {
                    Relationship rel = (Relationship) row.get("rel");
                    assertEquals("WORKS_FOR", rel.getType().name());
                });

        testCall(db, "match p=(:Company{name:'Neo4j'})<-[:DOCKING]-(d)<-[:WORKS_FOR]-(:Person{name:'Stefan'}) return d", map -> {
            Node dockingNode = (Node) map.get("d");
            assertTrue((int)dockingNode.getProperty("threadModulo", -1) > 0);
        });
    }

    @Test
    public void shouldDisconnectWork() {
        db.execute("create (c:Company{name:'Neo4j'}), (p:Person{name:'Stefan'}) with c,p " +
                        "call apoc.docking.connect(c, p, '<WORKS_FOR') yield rel return rel");


        long c = Iterators.count(db.execute("match (c:Company{name:'Neo4j'}), (p:Person{name:'Stefan'}) return count(*) as c").columnAs("c"));
        assertEquals(1, c);


        db.execute("match (c:Company{name:'Neo4j'}), (p:Person{name:'Stefan'}) " +
                "call apoc.docking.disconnect(c, p, '<WORKS_FOR') return null");

        testResult(db, "match p=(:Company{name:'Neo4j'})<-[:DOCKING]-(d)<-[:WORKS_FOR]-(:Person{name:'Stefan'}) return d", result -> {
            assertFalse(result.hasNext());
        });
    }

    @Test
    public void shouldRemoveWork() {
        db.execute("create (c:Company{name:'Neo4j'}), (p1:Person{name:'Stefan'}), (p2:Person{name:'John'}) with c,p " +
                "call apoc.docking.connect(c, p1, '<WORKS_FOR') yield rel as rel1 " +
                "call apoc.docking.connect(c, p2, '<WORKS_FOR') yield rel as rel2 " +
                "return rel1, rel2");


        db.execute("create (c:Company{name:'Neo4j'}) with c,p " +
                "call apoc.docking.remove(c) return null ");

        testResult(db, "match (c:Company{name:'Neo4j'}) return c", result -> {
            assertFalse(result.hasNext());
        });
    }
}
