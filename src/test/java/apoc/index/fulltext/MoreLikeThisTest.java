package apoc.index.fulltext;

import apoc.util.TestUtil;
import org.junit.Assert;
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

        db.execute("CALL db.index.fulltext.createNodeIndex('sampleIndex',['Quote'],['contents'],{analyzer:'german'})");

        db.execute("UNWIND $quotes as quote CREATE (:Quote{contents:quote})", MapUtil.map("quotes", quotes));
        db.execute("CALL db.index.fulltext.awaitEventuallyConsistentIndexRefresh()");

        Result r = db.execute("CALL db.index.fulltext.queryNodes('sampleIndex','Seiten')");
        List<Map<String, Object>> maps = Iterators.asList(r);

        r = db.execute("CALL apoc.index.fulltext.moreLikeThis('sampleIndex', 'contents', 'viele Seiten') YIELD node RETURN node.name");

        Assert.assertFalse(r.hasNext());



    }

}
