package apoc.index.fulltext;

import apoc.util.TestUtil;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

public class MoreLikeThisTest {

    private GraphDatabaseService db;

    public final List<String> quotes = Arrays.asList(
            "Das Buch hat viele Seiten.",
            "Ich habe viele Seiten gelesen in diesem Buch.",
            "Da werden wir andere Seiten aufziehen",
            "Da werden wir andere Saiten aufziehen"
    );

    @Before
    public void setUp() throws Exception {
        db = new TestGraphDatabaseFactory().newImpermanentDatabase();
        TestUtil.registerProcedure(db, MoreLikeThis.class);
    }

    @Test
    public void shouldMoreLikeThisWork() {
        db.execute("CALL db.index.fulltext.createNodeIndex('sampleIndex',['Quote'],['content'],{analyzer:'german'})");
        db.execute("UNWIND $quotes as quote CREATE (:Quote{content:quote})", MapUtil.map("quotes", quotes));
        db.execute("CALL db.index.fulltext.awaitEventuallyConsistentIndexRefresh()");

        Result r = db.execute("CALL apoc.index.fulltext.moreLikeThis('sampleIndex', ['content'], 'viele Seiten') YIELD node, score, rel RETURN *");
        List<Map<String, Object>> maps = Iterators.asList(r);
        assertEquals(3, maps.size());
        maps.forEach( map -> {
            Double score = (Double) map.get("score");
            assertTrue(score > 0 && score < 1);
            assertNotNull(map.get("node"));
            assertNull(map.get("rel"));
        });
    }

}
