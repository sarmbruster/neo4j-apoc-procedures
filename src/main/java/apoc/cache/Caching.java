package apoc.cache;

import apoc.Description;
import apoc.result.MapResult;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.helpers.collection.Pair;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * @author Stefan Armbruster
 */
public class Caching {

    /*
     * holds cypher + params as key -> maps to a materialized result
     */
    private static final TTLMap<Pair<String,Map<String,Object>>, List<Map<String,Object>>> cache = new TTLMapImpl<>();

    @Context
    public GraphDatabaseService graphDatabaseService;

    @Procedure
    @Description("call apoc.cache.cypherResults('match (n) return n.name as name, count(*) as count', {}, 10000) yield value return value.name as name, value.count as count")
    public Stream<MapResult> cypherResults(@Name("cypher") String cypher,
                                           @Name("params") Map<String, Object> params,
                                           @Name("ttl") long ttl) {
        Pair<String, Map<String, Object>> key = Pair.of(cypher, params);
        List<Map<String, Object>> cachedResult = cache.get(key);
        if (cachedResult==null) {
            Result result = graphDatabaseService.execute(cypher, params);
            cachedResult = Iterators.asList(result);
            cache.put(key, cachedResult, ttl);
        }
        return cachedResult.stream().map(stringObjectMap -> new MapResult(stringObjectMap));
    }

}
