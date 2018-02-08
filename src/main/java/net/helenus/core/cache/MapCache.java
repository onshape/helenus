package net.helenus.core.cache;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.configuration.CacheEntryListenerConfiguration;
import javax.cache.configuration.Configuration;
import javax.cache.integration.CompletionListener;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.EntryProcessorResult;

public class MapCache<K, V> implements Cache<K, V> {

  private Map<K, V> map = new HashMap<K, V>();

  @Override
  public V get(K key) {
    return map.get(key);
  }

  @Override
  public Map<K, V> getAll(Set<? extends K> keys) {
    Map<K, V> result = new HashMap<K, V>(keys.size());
    for (K key : keys) {
      V value = map.get(key);
      if (value != null) {
        result.put(key, value);
      }
    }
    return result;
  }

  @Override
  public boolean containsKey(K key) {
    return map.containsKey(key);
  }

  @Override
  public void loadAll(
      Set<? extends K> keys,
      boolean replaceExistingValues,
      CompletionListener completionListener) {}

  @Override
  public void put(K key, V value) {}

  @Override
  public V getAndPut(K key, V value) {
    return null;
  }

  @Override
  public void putAll(Map<? extends K, ? extends V> map) {}

  @Override
  public boolean putIfAbsent(K key, V value) {
    return false;
  }

  @Override
  public boolean remove(K key) {
    return false;
  }

  @Override
  public boolean remove(K key, V oldValue) {
    return false;
  }

  @Override
  public V getAndRemove(K key) {
    return null;
  }

  @Override
  public boolean replace(K key, V oldValue, V newValue) {
    return false;
  }

  @Override
  public boolean replace(K key, V value) {
    return false;
  }

  @Override
  public V getAndReplace(K key, V value) {
    return null;
  }

  @Override
  public void removeAll(Set<? extends K> keys) {}

  @Override
  public void removeAll() {}

  @Override
  public void clear() {}

  @Override
  public <C extends Configuration<K, V>> C getConfiguration(Class<C> clazz) {
    return null;
  }

  @Override
  public <T> T invoke(K key, EntryProcessor<K, V, T> entryProcessor, Object... arguments)
      throws EntryProcessorException {
    return null;
  }

  @Override
  public <T> Map<K, EntryProcessorResult<T>> invokeAll(
      Set<? extends K> keys, EntryProcessor<K, V, T> entryProcessor, Object... arguments) {
    return null;
  }

  @Override
  public String getName() {
    return null;
  }

  @Override
  public CacheManager getCacheManager() {
    return null;
  }

  @Override
  public void close() {}

  @Override
  public boolean isClosed() {
    return false;
  }

  @Override
  public <T> T unwrap(Class<T> clazz) {
    return null;
  }

  @Override
  public void registerCacheEntryListener(
      CacheEntryListenerConfiguration<K, V> cacheEntryListenerConfiguration) {}

  @Override
  public void deregisterCacheEntryListener(
      CacheEntryListenerConfiguration<K, V> cacheEntryListenerConfiguration) {}

  @Override
  public Iterator<Entry<K, V>> iterator() {
    return null;
  }
}
