package apoc.cache;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

/**
 * heavily inspired from https://gist.github.com/pcan/16faf4e59942678377e0
 * @author Stefan Armbruster
 */
public class TTLMapImpl<K, V> implements TTLMap<K, V> {

    private final Map<K, V> internalMap;
    private final Map<K, KeyWithTTL<K>> expiringKeys;

    /**
     * Holds the map keys using the given life time for expiration.
     */
    private final DelayQueue<KeyWithTTL> delayQueue = new DelayQueue<KeyWithTTL>();

    /**
     * The default max life time in milliseconds.
     */
    private final long maxLifeTimeMillis;

    public TTLMapImpl() {
        internalMap = new ConcurrentHashMap<K, V>();
        expiringKeys = new WeakHashMap<K, KeyWithTTL<K>>();
        this.maxLifeTimeMillis = Long.MAX_VALUE;
    }

    public TTLMapImpl(long defaultMaxLifeTimeMillis) {
        internalMap = new ConcurrentHashMap<K, V>();
        expiringKeys = new WeakHashMap<K, KeyWithTTL<K>>();
        this.maxLifeTimeMillis = defaultMaxLifeTimeMillis;
    }

    public TTLMapImpl(long defaultMaxLifeTimeMillis, int initialCapacity) {
        internalMap = new ConcurrentHashMap<K, V>(initialCapacity);
        expiringKeys = new WeakHashMap<K, KeyWithTTL<K>>(initialCapacity);
        this.maxLifeTimeMillis = defaultMaxLifeTimeMillis;
    }

    public TTLMapImpl(long defaultMaxLifeTimeMillis, int initialCapacity, float loadFactor) {
        internalMap = new ConcurrentHashMap<K, V>(initialCapacity, loadFactor);
        expiringKeys = new WeakHashMap<K, KeyWithTTL<K>>(initialCapacity, loadFactor);
        this.maxLifeTimeMillis = defaultMaxLifeTimeMillis;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int size() {
        cleanup();
        return internalMap.size();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isEmpty() {
        cleanup();
        return internalMap.isEmpty();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean containsKey(Object key) {
        cleanup();
        return internalMap.containsKey((K) key);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean containsValue(Object value) {
        cleanup();
        return internalMap.containsValue((V) value);
    }

    @Override
    public V get(Object key) {
        cleanup();
        renewKey((K) key);
        return internalMap.get((K) key);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public V put(K key, V value) {
        return this.put(key, value, maxLifeTimeMillis);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public V put(K key, V value, long lifeTimeMillis) {
        cleanup();
        KeyWithTTL delayedKey = new KeyWithTTL(key, lifeTimeMillis);
        expireKey(expiringKeys.put((K) key, delayedKey));
        delayQueue.offer(delayedKey);
        return internalMap.put(key, value);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public V remove(Object key) {
        expireKey(expiringKeys.remove((K) key));
        return internalMap.remove((K) key);
    }

    /**
     * Not supported.
     */
    @Override
    public void putAll(Map<? extends K, ? extends V> m) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void renewKey(K key) {
        KeyWithTTL<K> delayedKey = expiringKeys.get((K) key);
        if (delayedKey != null) {
            delayedKey.renew();
        }
    }

    private void expireKey(KeyWithTTL<K> delayedKey) {
        if (delayedKey != null) {
            delayedKey.expire();
            delayQueue.poll();
        }
    }

    @Override
    public void clear() {
        delayQueue.clear();
        expiringKeys.clear();
        internalMap.clear();
    }

    /**
     * Not supported.
     */
    @Override
    public Set<K> keySet() {
        throw new UnsupportedOperationException();
    }

    /**
     * Not supported.
     */
    @Override
    public Collection<V> values() {
        throw new UnsupportedOperationException();
    }

    /**
     * Not supported.
     */
    @Override
    public Set<Entry<K, V>> entrySet() {
        throw new UnsupportedOperationException();
    }

    private void cleanup() {
        KeyWithTTL<K> delayedKey = delayQueue.poll();
        while (delayedKey != null) {
            internalMap.remove(delayedKey.getKey());
            expiringKeys.remove(delayedKey.getKey());
            delayedKey = delayQueue.poll();
        }
    }

    private class KeyWithTTL<K> implements Delayed {

        private long startTime = System.currentTimeMillis();
        private final long maxLifeTimeMillis;
        private final K key;

        public KeyWithTTL(K key, long maxLifeTimeMillis) {
            this.maxLifeTimeMillis = maxLifeTimeMillis;
            this.key = key;
        }

        public K getKey() {
            return key;
        }

        @Override
        public long getDelay(TimeUnit unit) {
            return unit.convert(getDelayMillis(), TimeUnit.MILLISECONDS);
        }

        private long getDelayMillis() {
            return (startTime + maxLifeTimeMillis) - System.currentTimeMillis();
        }

        public void renew() {
            startTime = System.currentTimeMillis();
        }

        public void expire() {
            startTime = System.currentTimeMillis() - maxLifeTimeMillis - 1;
        }

        @Override
        public int compareTo(Delayed that) {
            return Long.compare(this.getDelayMillis(), ((KeyWithTTL) that).getDelayMillis());
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            KeyWithTTL<?> that = (KeyWithTTL<?>) o;

            if (startTime != that.startTime) return false;
            if (maxLifeTimeMillis != that.maxLifeTimeMillis) return false;
            return key != null ? key.equals(that.key) : that.key == null;

        }

        @Override
        public int hashCode() {
            int result = (int) (startTime ^ (startTime >>> 32));
            result = 31 * result + (int) (maxLifeTimeMillis ^ (maxLifeTimeMillis >>> 32));
            result = 31 * result + (key != null ? key.hashCode() : 0);
            return result;
        }
    }
}