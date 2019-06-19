package apoc.index.fulltext;

import apoc.util.Util;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.internal.kernel.api.IndexReference;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotFoundKernelException;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.impl.fulltext.FulltextAdapter;
import org.neo4j.kernel.api.impl.fulltext.LuceneFulltextDocumentStructure;
import org.neo4j.kernel.impl.newapi.AllStoreHolder;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;
import org.neo4j.storageengine.api.EntityType;
import org.neo4j.storageengine.api.schema.IndexReader;

import java.io.IOException;
import java.io.StringReader;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Stream;

public class MoreLikeThis {

    @Context
    public GraphDatabaseService db;

    @Context
    public KernelTransaction tx;

    @Context
    public FulltextAdapter accessor;

    @Procedure
    public Stream<ScoredNodeOrRelationshipResult> moreLikeThis(@Name("indexName") String indexName,
                                           @Name("propertyKey") String propertyKey,
                                           @Name("term") String term,
                                           @Name(value="config", defaultValue = "{}") Map<String,Object> config) throws IOException {

        LuceneFulltextIndexInfo lfii = new LuceneFulltextIndexInfo(tx, indexName);
        IndexSearcher indexSearcher = lfii.getIndexSearcher();
        final org.apache.lucene.index.IndexReader luceneIndexReader = indexSearcher.getIndexReader();

        org.apache.lucene.queries.mlt.MoreLikeThis mlt = new org.apache.lucene.queries.mlt.MoreLikeThis(luceneIndexReader);
        mlt.setAnalyzer(lfii.getAnalyzer());
        mlt.setMinTermFreq(Util.toInteger(config.getOrDefault("minTermFreq", "1")));
        mlt.setMinDocFreq(Util.toInteger(config.getOrDefault("minDocFreq", "1")));

        Query query = mlt.like(propertyKey, new StringReader(term));
        TopDocs topDocs = indexSearcher.search(query, Util.toInteger(config.getOrDefault("maxResults", "10")));

        return Arrays.stream(topDocs.scoreDocs).map(scoreDoc -> {
            try {
                Document document = luceneIndexReader.document(scoreDoc.doc);
                IndexableField field = document.getField(LuceneFulltextDocumentStructure.FIELD_ENTITY_ID);
                long id = Util.toLong(field.stringValue());
                return new ScoredNodeOrRelationshipResult(new Float(scoreDoc.score).doubleValue(),
                        lfii.getEntityType().equals(EntityType.NODE) ? db.getNodeById(Util.toLong(id)) : null,
                        lfii.getEntityType().equals(EntityType.RELATIONSHIP) ? db.getRelationshipById(Util.toLong(id)) : null);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    public static class ScoredNodeOrRelationshipResult {
        final public Double score;
        final public Node node;
        final public Relationship relationship;

        public ScoredNodeOrRelationshipResult(Double score, Node node, Relationship relationship) {
            this.score = score;
            this.node = node;
            this.relationship = relationship;
        }
    }

    public static class LuceneFulltextIndexInfo {

        private final IndexReader indexReader;
        private final Field analyzerField;
        private final Method getIndexSearcherMethod;
        private final EntityType entityType;

        public LuceneFulltextIndexInfo(KernelTransaction tx, String indexName) {
            try {
                IndexReference indexReference = tx.schemaRead().indexGetForName(indexName);
                entityType = indexReference.schema().entityType();

                AllStoreHolder allStoreHolder = (AllStoreHolder) tx.dataRead();
                indexReader = allStoreHolder.indexReader(indexReference, false);

                Class<?> simpleFulltextIndexReaderClass = Class.forName("org.neo4j.kernel.api.impl.fulltext.SimpleFulltextIndexReader");
                getIndexSearcherMethod = simpleFulltextIndexReaderClass.getDeclaredMethod("getIndexSearcher");
                getIndexSearcherMethod.setAccessible(true);

                analyzerField = simpleFulltextIndexReaderClass.getDeclaredField("analyzer");
                analyzerField.setAccessible(true);

            } catch (ClassNotFoundException | NoSuchFieldException | IndexNotFoundKernelException | NoSuchMethodException e) {
                throw new RuntimeException(e);
            }
        }

        private Object invoke(Method m, Object args) {
            try {
                return m.invoke(args);
            } catch (IllegalAccessException|InvocationTargetException e) {
                throw new RuntimeException(e);
            }
        }

        public IndexSearcher getIndexSearcher() {
            return (IndexSearcher) invoke(getIndexSearcherMethod, indexReader);
        }

        public Analyzer getAnalyzer() {
            try {
                return (Analyzer) analyzerField.get(indexReader);
            } catch (IllegalAccessException e) {
                throw new RuntimeException(e);
            }
        }

        public EntityType getEntityType() {
            return entityType;
        }
    }

}
