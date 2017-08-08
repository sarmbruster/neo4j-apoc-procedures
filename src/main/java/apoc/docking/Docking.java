package apoc.docking;

import apoc.result.RelationshipResult;
import org.neo4j.graphdb.*;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static org.neo4j.graphdb.Direction.INCOMING;
import static org.neo4j.graphdb.Direction.OUTGOING;
import static org.neo4j.procedure.Mode.WRITE;

/**
 * manage docking nodes based on modulo-threadid operations to prevent locking fuckups
 */
public class Docking {

    @Context
    public GraphDatabaseService graphDatabaseService;

    private static RelationshipType DOCKING = RelationshipType.withName("DOCKING");
    private static int NUMBER_OF_DOCKING_NODES = 16;

    @Procedure(mode = WRITE)
    public Stream<RelationshipResult> connect(@Name("denseNode") Node dense, @Name("targetNode") Node target,
                                              @Name("directionAndRelType") String directionAndRelType) {
        Direction direction = getDirection(directionAndRelType);
        RelationshipType relationshipType = getRelationshipType(directionAndRelType);
        Relationship rel = target.getSingleRelationship(relationshipType, direction);
        if (rel!=null) {
            Node dockingNode = rel.getOtherNode(target);
            Relationship dockingRel = dockingNode.getSingleRelationship(DOCKING, OUTGOING);
            if (dockingRel!=null) {
                final Node otherNode = dockingRel.getOtherNode(dockingNode);
                if (otherNode.equals(dense)) {
                    return Stream.of(new RelationshipResult(rel));
                } else {
                    throw new IllegalArgumentException("weird. target node is connected to another denseNode " + otherNode );
                }
            } else {
                throw new IllegalArgumentException("weird. target node is connected to a docking node (" + dockingNode +
                        ") that lacks connection to any dense node");
            }
        } else {
            int threadId = (int) (Thread.currentThread().getId() % NUMBER_OF_DOCKING_NODES);
            final Optional<Node> dockingNodeOptional = StreamSupport.stream(target.getRelationships(DOCKING, INCOMING).spliterator(), false)
                    .map(relationship -> relationship.getStartNode())
                    .filter(node -> (int) (node.getProperty("threadModulo", -1)) == threadId)
                    .findFirst();
            Node dockingNode = dockingNodeOptional.orElseGet(() -> {
                Node n = graphDatabaseService.createNode();
                n.setProperty("threadModulo", threadId);
                n.createRelationshipTo(dense, DOCKING);
                return n;
            });
            Node start = direction==INCOMING ? dockingNode : target;
            Node end = direction==INCOMING ? target : dockingNode;
            return Stream.of(new RelationshipResult(start.createRelationshipTo(end, relationshipType)));
        }
    }

    private RelationshipType getRelationshipType(@Name("directionAndRelType") String directionAndRelType) {
        return RelationshipType.withName(directionAndRelType.substring(1));
    }

    private Direction getDirection(@Name("directionAndRelType") String directionAndRelType) {
        char directionChar = directionAndRelType.charAt(0);
        switch (directionChar) {
            case '<': return OUTGOING;
            case '>': return INCOMING;
            default:
                throw new IllegalArgumentException("directionAndRelType must start with '<' or '>'");
        }
    }

    @Procedure(mode = WRITE)
    public void disconnect(@Name("denseNode") Node dense, @Name("targetNode") Node target,
                           @Name("directionAndRelType") String directionAndRelType) {
        Direction direction = getDirection(directionAndRelType);
        RelationshipType relationshipType = getRelationshipType(directionAndRelType);
        final List<Relationship> collect = StreamSupport.stream(target.getRelationships(relationshipType, direction).spliterator(), false).collect(Collectors.toList());
        for (Relationship r: collect) {
            r.delete();
        }
    }
}
