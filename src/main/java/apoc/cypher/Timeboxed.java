package apoc.cypher;

import apoc.Pools;
import apoc.result.MapResult;
import org.neo4j.graphdb.*;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * @author mh
 * @since 20.02.18
 */
public class Timeboxed {

    @Context
    public GraphDatabaseService db;

    @Context
    public Log log;

    private final static Map<String,Object> POISON = Collections.singletonMap("__magic", "POISON");

    @Procedure
    @Description("apoc.cypher.runTimeboxed('cypherStatement',{params}, timeout) - abort kernelTransaction after timeout ms if not finished")
    public Stream<MapResult> runTimeboxed(@Name("cypher") String cypher, @Name("params") Map<String, Object> params,
                                          @Name("timeout") long timeout,
                                          @Name(value = "sendStatusAndError", defaultValue = "false") Boolean sendStatusAndError) {

        final BlockingQueue<Map<String, Object>> queue = new ArrayBlockingQueue<>(100);
        final AtomicReference<Transaction> txAtomic = new AtomicReference<>();

        final AtomicReference<State> overallState = new AtomicReference<>(State.COMPLETE);
        final AtomicReference<Exception> cypherException = new AtomicReference<>(null);
        log.debug("starting run timeboxed");

        // run query to be timeboxed in a separate thread to enable proper tx termination
        // if we'd run this in current thread, a tx.terminate would kill the transaction the procedure call uses itself.
        Pools.DEFAULT.submit(() -> {
            try (Transaction tx = db.beginTx()) {
                txAtomic.set(tx);
                Result result = db.execute(cypher, params == null ? Collections.EMPTY_MAP : params);
                while (result.hasNext()) {
                    final Map<String, Object> map = result.next();
                    offerToQueue(queue, map, timeout);
                }
                tx.success();
            } catch (QueryExecutionException e) {
                log.error("cypher statement raised exception " + e.getMessage());
                overallState.set(State.ERROR);
                cypherException.set(e);
//            } catch (TransactionTerminatedException e) {
//                log.warn("query " + cypher + " has been terminated");
            } catch (TransientTransactionFailureException e) {
                if (e.getCause() instanceof TransactionTerminatedException) {
                    log.info("transaction terminated by user");
                } else  {
                    log.error(e.getMessage(), e);
                }
            } catch (Exception e) {
                log.error("oops - non expected exception", e);
            } finally {
                offerToQueue(queue, POISON, timeout);
                log.debug("finishing worker thread - sent POISON to queue");
                txAtomic.set(null);
            }
        });

        //
        Pools.SCHEDULED.schedule(() -> {
            log.debug("started terminator thread");
            Transaction tx = txAtomic.get();
            if (tx==null) {
                log.info("tx is null, either the other transaction finished gracefully or has not yet been started.");
            } else {
                tx.terminate();
                overallState.set(State.TERMINATED);
                offerToQueue(queue, POISON, timeout);
                log.warn("terminating transaction due to timeout, putting POISON onto queue");
            }
        }, timeout, MILLISECONDS);

        // consume the blocking queue using a custom iterator finishing upon POISON
        Iterator<Map<String,Object>> queueConsumer = new Iterator<Map<String, Object>>() {
            Map<String,Object> nextElement = null;
            boolean hasFinished = false;

            @Override
            public boolean hasNext() {
                if (hasFinished) {
                    return false;
                } else {
                    try {
                        // we need to wait longer than timeout. Otherwise killing might occur before this thread gets aware of it
                        log.debug("hasNext - polling");
                        nextElement = queue.poll(timeout + 100, MILLISECONDS);
                        if (nextElement==null) {

                            // we need to check if terminator thread has already modified overallState
                            // if so, we don't touch it here
                            if (overallState.get()==State.COMPLETE) {
                                overallState.set(State.TIMEOUT);
                                log.warn("couldn't grab queue element within timeout of %d millis - aborting.", timeout);
                            } else {
                                log.warn("couldn't grab queue element within timeout of %d millis - state " + overallState.get());
                            }
                            hasFinished = true;
                        } else {
                            log.debug("retrieved POISON");
                            hasFinished = POISON.equals(nextElement);
                        }
                        log.debug("hasFinished: " +hasFinished);
                        return !hasFinished;
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
            }

            @Override
            public Map<String, Object> next() {
                return nextElement;
            }
        };

        Spliterator<Map<String, Object>> spliterator = Spliterators.spliteratorUnknownSize(queueConsumer, Spliterator.ORDERED);
        final AtomicReference<Map<String, Object>> reference = new AtomicReference<>();

        Stream<Map<String, Object>> stream = Stream.empty();
        if (spliterator.tryAdvance(reference::set)) {
            stream = Stream.concat(Stream.of(reference.get()), StreamSupport.stream(spliterator, false));
        } else {
            if (sendStatusAndError) {
                State state = overallState.get();
                String errorText = null;
                if (state == State.ERROR) {
                    errorText = cypherException.get().getMessage();
                }
                stream = Stream.of(MapUtil.map("status", state.toString(), "error", errorText));
            }
        }
        return stream.map(MapResult::new);
    }

    private void offerToQueue(BlockingQueue<Map<String, Object>> queue, Map<String, Object> map, long timeout)  {
        try {
            boolean hasBeenAdded = queue.offer(map, timeout, MILLISECONDS);
            if (!hasBeenAdded) {
                log.error( "adding timed out");
                throw new IllegalStateException("couldn't add a value to a queue of size " + queue.size() + ". Either increase capacity or fix consumption of the queue");
            }
            log.info("put into queue " + queue.size());
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public enum State {
        COMPLETE,
        TIMEOUT,
        TERMINATED,
        ERROR
    }
}
