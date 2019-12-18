package apoc.systemdb;

import apoc.ApocConfig;
import apoc.create.Create;
import apoc.result.*;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.procedure.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class SystemDb {

    @Context
    public ApocConfig apocConfig;

    public static class NodesAndRelationshipsResult {
        public List<Node> nodes;
        public List<Relationship> relationships;

        public NodesAndRelationshipsResult(List<Node> nodes, List<Relationship> relationships) {
            this.nodes = nodes;
            this.relationships = relationships;
        }
    }

    @Procedure
    public Stream<NodesAndRelationshipsResult> graph() {
        return withSystemDbTransaction(tx -> {
            Map<Long, Node> virtualNodes = new HashMap<>();
            for (Node node: tx.getAllNodes())  {
                virtualNodes.put(-node.getId(), virtualNodeOf(node));
            }

            List<Relationship> relationships = tx.getAllRelationships().stream().map(rel -> virtualRelationshipOf(rel, virtualNodes.get(-rel.getStartNodeId()), virtualNodes.get(-rel.getEndNodeId()))).collect(Collectors.toList()
            );
            return Stream.of(new NodesAndRelationshipsResult(Iterables.asList(virtualNodes.values()), relationships) );
        });
    }

    @Procedure
    public Stream<RowResult> execute(@Name("DDL command") String command, @Name(value="params", defaultValue = "{}") Map<String ,Object> params) {
        return withSystemDbTransaction(tx -> tx.execute(command, params).stream().map(map -> new RowResult(map)));
    }

    @Procedure(mode = Mode.WRITE, name="apoc.systemdb.create.node")
    @Description("apoc.systemdb.create.node(['Label'], {key:value,...}) - create node in systemdb")
    public Stream<NodeResult> createNode(@Name("label") List<String> labelNames, @Name("props") Map<String, Object> props) {
        return withSystemDbTransaction( tx -> {
            Stream<NodeResult> stream = new Create(tx).node(labelNames, props);
            List<NodeResult> materialized = stream.map(nodeResult -> new NodeResult(virtualNodeOf(nodeResult.node))).collect(Collectors.toList());
            return materialized.stream();
        });
    }

    @Procedure(mode = Mode.WRITE, name="apoc.systemdb.create.relationship")
    @Description("apoc.systemdb.create.relationship(person1,'KNOWS',{key:value,...}, person2) create relationship in systemdb")
    public Stream<RelationshipResult> relationship(@Name("from") long from,
                                                   @Name("relType") String relType, @Name("props") Map<String, Object> props,
                                                   @Name("to") long to) {
        return withSystemDbTransaction(tx -> {
            Node fromNode = tx.getNodeById(Math.abs(from));
            Node toNode = tx.getNodeById(Math.abs(to));
            Stream<RelationshipResult> stream = new Create(tx).relationship(fromNode, relType, props, toNode);
            List<RelationshipResult> materialized = stream.map(relationshipResult -> new RelationshipResult(virtualRelationshipOf(relationshipResult.rel, fromNode, toNode))).collect(Collectors.toList());
            return materialized.stream();
        });
    }

    private VirtualNode virtualNodeOf(Node node) {
        return new VirtualNode(-node.getId(), Iterables.asArray(Label.class, node.getLabels()), node.getAllProperties());
    }

    private VirtualRelationship virtualRelationshipOf(Relationship rel, Node startNode, Node endNode) {
        return new VirtualRelationship(
                -rel.getId(),
                startNode,
                endNode,
                rel.getType(),
                rel.getAllProperties());
    }

    private <T> T withSystemDbTransaction(Function<Transaction, T> function) {
        try (Transaction tx = apocConfig.getSystemDb().beginTx()) {
            T result = function.apply(tx);
            tx.commit();
            return result;
        }
    }
}
