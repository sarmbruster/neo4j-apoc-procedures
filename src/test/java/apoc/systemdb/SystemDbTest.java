package apoc.systemdb;

import apoc.util.TestUtil;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.ResultTransformer;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.internal.helpers.collection.MapUtil;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.Assert.*;

public class SystemDbTest {

    @Rule
    public DbmsRule db = new ImpermanentDbmsRule();

    @Before
    public void setUp() throws Exception {
        TestUtil.registerProcedure(db, SystemDb.class);
    }

    @Test
    public void testGetGraph() throws Exception {
        TestUtil.testResult(db, "CALL apoc.systemdb.graph() YIELD nodes, relationships RETURN nodes, relationships", result -> {
            Map<String, Object> map = Iterators.single(result);
            List<Node> nodes = (List<Node>) map.get("nodes");
            List<Relationship> relationships = (List<Relationship>) map.get("relationships");

            assertEquals(2, nodes.size());
            assertTrue( nodes.stream().allMatch( node -> "Database".equals(Iterables.single(node.getLabels()).name())));
            List<String> names = nodes.stream().map(node -> (String)node.getProperty("name")).collect(Collectors.toList());
            assertThat( names, Matchers.containsInAnyOrder("neo4j", "system"));

            assertTrue(relationships.isEmpty());
        });
    }

    @Test
    public void testExecute() {
        TestUtil.testResult(db, "CALL apoc.systemdb.create.node('SHOW DATABASES') YIELD row RETURN row", result -> {
            List<Map<String, Object>> rows = Iterators.asList(result.columnAs("row"));
            assertThat(rows, Matchers.containsInAnyOrder(
                    MapUtil.map("name", "system", "default", false, "currentStatus", "online", "role", "standalone", "requestedStatus", "online", "error", "", "address", "localhost:7687"),
                    MapUtil.map("name", "neo4j", "default", true, "currentStatus", "online", "role", "standalone", "requestedStatus", "online", "error", "", "address", "localhost:7687")
            ));
        });
    }

    @Test
    public void testCreateNode() {
        TestUtil.testResult(db, "CALL apoc.systemdb.create.node(['Database'], {name:'second', status:'online', default:false, uuid:randomUUID()})",
                result -> {
                    Node node = (Node) Iterators.single(result).get("node");
                    assertTrue(node.hasLabel(Label.label("Database")));
                    assertEquals("second", node.getProperty("name"));
                });
    }

    @Test
    public void testCreateRelationship() {
        ResultTransformer<Object> nodeTransformer = result -> Iterators.single(result).get("node");
        Node john = (Node) db.executeTransactionally("CALL apoc.systemdb.create.node(['Person'], {name:'John'}) yield node return node", Collections.emptyMap(), nodeTransformer);
        Node jim = (Node) db.executeTransactionally("CALL apoc.systemdb.create.node(['Person'], {name:'Jim'}) yield node return node", Collections.emptyMap(), nodeTransformer);

        TestUtil.testResult(db, "CALL apoc.systemdb.create.relationship($john, 'KNOWS', {}, $jim) yield rel return rel",
                MapUtil.map("john", john.getId(), "jim", jim.getId()),
                result -> {
                    Relationship rel = (Relationship) Iterators.single(result).get("rel");
                    assertEquals("KNOWS", rel.getType().name());
                });

        long count= TestUtil.singleResultFirstColumn(db, "CALL apoc.systemdb.graph() YIELD relationships UNWIND relationships AS rel WITH rel WHERE type(rel)='KNOWS' RETURN count(*) as count");
        assertEquals(1L, count);
    }
}
