package net.helenus.core.cache;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.configuration.CacheEntryListenerConfiguration;
import javax.cache.configuration.Configuration;
import javax.cache.event.CacheEntryRemovedListener;
import javax.cache.integration.CacheLoader;
import javax.cache.integration.CompletionListener;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.EntryProcessorResult;
import javax.cache.processor.MutableEntry;

public class MapCache<K, V> implements Cache<K, V> {
  private final CacheManager manager;
  private final String name;
  private Map<K, V> map = new ConcurrentHashMap<>();
  private Set<CacheEntryRemovedListener<K, V>> cacheEntryRemovedListeners = new HashSet<>();
  private CacheLoader<K, V> cacheLoader = null;
  private boolean isReadThrough = false;

  private static class MapConfiguration<K, V> implements Configuration<K, V> {
    private static final long serialVersionUID = 6093947542772516209L;

    @Override
    public Class<K> getKeyType() {
      return null;
    }

    @Override
    public Class<V> getValueType() {
      return null;
    }

    @Override
    public boolean isStoreByValue() {
      return false;
    }
  }

  public MapCache(
      CacheManager manager, String name, CacheLoader<K, V> cacheLoader, boolean isReadThrough) {
    this.manager = manager;
    this.name = name;
    this.cacheLoader = cacheLoader;
    this.isReadThrough = isReadThrough;
  }


  /** {@inheritDoc} */
  @Override
  public V get(K key) {
    V value = null;
    synchronized (map) {
      value = map.get(key);
      if (value == null && isReadThrough && cacheLoader != null) {
        V loadedValue = cacheLoader.load(key);
        if (loadedValue != null) {
          map.put(key, loadedValue);
          value = loadedValue;
        }
      }
    }
    return value;
  }

  /** {@inheritDoc} */
  @Override
  public Map<K, V> getAll(Set<? extends K> keys) {
    Map<K, V> result = null;
    synchronized (map) {
      result = new HashMap<K, V>(keys.size());
      Iterator<? extends K> it = keys.iterator();
      while (it.hasNext()) {
        K key = it.next();
        V value = map.get(key);
        if (value != null) {
          result.put(key, value);
          it.remove();
        }
      }
      if (keys.size() != 0 && isReadThrough && cacheLoader != null) {
        Map<K, V> loadedValues = cacheLoader.loadAll(keys);
        for (Map.Entry<K, V> entry : loadedValues.entrySet()) {
          V v = entry.getValue();
          if (v != null) {
            K k = entry.getKey();
            map.put(k, v);
            result.put(k, v);
          }
        }
      }
    }
    return result;
  }

  /** {@inheritDoc} */
  @Override
  public boolean containsKey(K key) {
    return map.containsKey(key);
  }

  /** {@inheritDoc} */
  @Override
  public void loadAll(
      Set<? extends K> keys, boolean replaceExistingValues, CompletionListener completionListener) {
    if (cacheLoader != null) {
      try {
        synchronized (map) {
          Map<K, V> loadedValues = cacheLoader.loadAll(keys);
          for (Map.Entry<K, V> entry : loadedValues.entrySet()) {
            V value = entry.getValue();
            K key = entry.getKey();
            if (value != null) {
              boolean existsCurrently = map.containsKey(key);
              if (!existsCurrently || replaceExistingValues) {
                map.put(key, value);
                keys.remove(key);
              }
            }
          }
        }
      } catch (Exception e) {
        if (completionListener != null) {
          completionListener.onException(e);
        }
      }
    }
    if (completionListener != null) {
      if (keys.isEmpty()) {
        completionListener.onCompletion();
      }
    }
  }

  /** {@inheritDoc} */
  @Override
  public void put(K key, V value) {
    map.put(key, value);
  }

  /** {@inheritDoc} */
  @Override
  public V getAndPut(K key, V value) {
    V result = null;
    synchronized (map) {
      result = map.get(key);
      if (result == null && isReadThrough && cacheLoader != null) {
        V loadedValue = cacheLoader.load(key);
        if (loadedValue != null) {
          result = loadedValue;
        }
      }
      map.put(key, value);
    }
    return result;
  }

  /** {@inheritDoc} */
  @Override
  public void putAll(Map<? extends K, ? extends V> map) {
    synchronized (map) {
      for (Map.Entry<? extends K, ? extends V> entry : map.entrySet()) {
        this.map.put(entry.getKey(), entry.getValue());
      }
    }
  }

  /** {@inheritDoc} */
  @Override
  public boolean putIfAbsent(K key, V value) {
    synchronized (map) {
      if (!map.containsKey(key)) {
        map.put(key, value);
        return true;
      } else {
        return false;
      }
    }
  }

  /** {@inheritDoc} */
  @Override
  public boolean remove(K key) {
    boolean removed = false;
    synchronized (map) {
      removed = map.remove(key) != null;
      notifyRemovedListeners(key);
    }
    return removed;
  }

  /** {@inheritDoc} */
  @Override
  public boolean remove(K key, V oldValue) {
    synchronized (map) {
      V value = map.get(key);
      if (value != null && oldValue.equals(value)) {
        map.remove(key);
        notifyRemovedListeners(key);
        return true;
      }
    }
    return false;
  }

  /** {@inheritDoc} */
  @Override
  public V getAndRemove(K key) {
    synchronized (map) {
      V oldValue = null;
      oldValue = map.get(key);
      map.remove(key);
      notifyRemovedListeners(key);
      return oldValue;
    }
  }

  /** {@inheritDoc} */
  @Override
  public boolean replace(K key, V oldValue, V newValue) {
    synchronized (map) {
      V value = map.get(key);
      if (value != null && oldValue.equals(value)) {
        map.put(key, newValue);
        return true;
      }
    }
    return false;
  }

  /** {@inheritDoc} */
  @Override
  public boolean replace(K key, V value) {
    synchronized (map) {
      if (map.containsKey(key)) {
        map.put(key, value);
        return true;
      }
    }
    return false;
  }

  /** {@inheritDoc} */
  @Override
  public V getAndReplace(K key, V value) {
    synchronized (map) {
      V oldValue = map.get(key);
      if (value != null && value.equals(oldValue)) {
        map.put(key, value);
        return oldValue;
      }
    }
    return null;
  }

  /** {@inheritDoc} */
  @Override
  public void removeAll(Set<? extends K> keys) {
    synchronized (map) {
      Iterator<? extends K> it = keys.iterator();
      while (it.hasNext()) {
        K key = it.next();
        if (map.containsKey(key)) {
          map.remove(key);
        } else {
          it.remove();
        }
      }
    }
    notifyRemovedListeners(keys);
  }

  /** {@inheritDoc} */
  @Override
  public void removeAll() {
    synchronized (map) {
      Set<K> keys = map.keySet();
      map.clear();
      notifyRemovedListeners(keys);
    }
  }

  /** {@inheritDoc} */
  @Override
  public void clear() {
    map.clear();
  }

  /** {@inheritDoc} */
  @Override
  public <C extends Configuration<K, V>> C getConfiguration(Class<C> clazz) {
    if (!MapConfiguration.class.isAssignableFrom(clazz)) {
      throw new IllegalArgumentException();
    }
    return null;
  }

  /** {@inheritDoc} */
  @Override
  public <T> T invoke(K key, EntryProcessor<K, V, T> entryProcessor, Object... arguments)
      throws EntryProcessorException {
    // TODO
    return null;
  }

  /** {@inheritDoc} */
  @Override
  public <T> Map<K, EntryProcessorResult<T>> invokeAll(
      Set<? extends K> keys, EntryProcessor<K, V, T> entryProcessor, Object... arguments) {
    synchronized (map) {
      for (K key : keys) {
        V value = map.get(key);
        if (value != null) {
          entryProcessor.process(
              new MutableEntry<K, V>() {
                @Override
                public boolean exists() {
                  return map.containsKey(key);
                }

                @Override
                public void remove() {
                  synchronized (map) {
                    V value = map.get(key);
                    if (value != null) {
                      map.remove(key);
                      notifyRemovedListeners(key);
                    }
                  }
                }

                @Override
                public K getKey() {
                  return key;
                }

                @Override
                public V getValue() {
                  return map.get(value);
                }

                @Override
                public <T> T unwrap(Class<T> clazz) {
                  return null;
                }

                @Override
                public void setValue(V value) {
                  map.put(key, value);
                }
              },
              arguments);
        }
      }
    }
    return null;
  }

  /** {@inheritDoc} */
  @Override
  public String getName() {
    return name;
  }

  /** {@inheritDoc} */
  @Override
  public CacheManager getCacheManager() {
    return manager;
  }

  /** {@inheritDoc} */
  @Override
  public void close() {}

  /** {@inheritDoc} */
  @Override
  public boolean isClosed() {
    return false;
  }

  /** {@inheritDoc} */
  @Override
  @SuppressWarnings("unchecked")
  public <T> T unwrap(Class<T> clazz) {
    if (Map.class.isAssignableFrom(clazz)) {
        return (T) map;
    }
    return null;
  }

  /** {@inheritDoc} */
  @Override
  public void registerCacheEntryListener(
      CacheEntryListenerConfiguration<K, V> cacheEntryListenerConfiguration) {
    //cacheEntryRemovedListeners.add(cacheEntryListenerConfiguration.getCacheEntryListenerFactory().create());
  }

  /** {@inheritDoc} */
  @Override
  public void deregisterCacheEntryListener(
      CacheEntryListenerConfiguration<K, V> cacheEntryListenerConfiguration) {}

  /** {@inheritDoc} */
  @Override
  public Iterator<Entry<K, V>> iterator() {
    synchronized (map) {
      Cache<K, V> self = this;

      return new Iterator<Entry<K, V>>() {

        Cache<K, V> cache = self;
        Cache.Entry<K, V> lastEntry;
        Iterator<Map.Entry<K, V>> entries = map.entrySet().iterator();

        @Override
        public boolean hasNext() {
          return entries.hasNext();
        }

        @Override
        public Cache.Entry<K, V> next() {
          Map.Entry<K, V> entry = entries.next();
          lastEntry = new Cache.Entry<K, V>() {
            K key = (K) entry.getKey();
            V value = (V) entry.getValue();

            @Override
            public K getKey() {
              return key;
            }

            @Override
            public V getValue() {
              return value;
            }

            @Override
            public <T> T unwrap(Class<T> clazz) {
              if (Map.Entry.class.isAssignableFrom(clazz)) {
                  return (T) entry;
              }
              return null;
            }
          };
          return lastEntry;
        }

        @Override
        public void remove() {
            if (lastEntry != null) {
                cache.remove((K) lastEntry.getKey());
            }
        }
      };
    }
  }

  private void notifyRemovedListeners(K key) {
    //      if (cacheEntryRemovedListeners != null) {
    //          cacheEntryRemovedListeners.forEach(listener -> listener.onRemoved())
    //      }
  }

  private void notifyRemovedListeners(Set<? extends K> keys) {}
}
