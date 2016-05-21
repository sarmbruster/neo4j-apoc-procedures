package apoc.cache;

import java.util.Map;

/**
 * heavily inspired from https://gist.github.com/pcan/16faf4e59942678377e0
 * @author Stefan Armbruster
 */
public interface TTLMap<K, V> extends Map<K, V> {

    /**
     * Renews the specified key, setting the life time to the initial value.
     *
     * @param key
     * @return true if the key is found, false otherwise
     */
     void renewKey(K key);

    /**
     * like {@link Map#put(Object, Object)} but with a definition of a TTL
     *
     * @param key
     * @param value
     * @param ttl time to live in millis
     * @return a previously associated object for the given key (if exists).
     */
    V put(K key, V value, long ttl);

}