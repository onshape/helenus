/*
 *      Copyright (C) 2015 The Helenus Authors
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package net.helenus.core;

import static net.helenus.core.HelenusSession.deleted;

import com.google.common.base.Stopwatch;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import com.google.common.collect.TreeTraverser;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.configuration.CacheEntryListenerConfiguration;
import javax.cache.configuration.Configuration;
import javax.cache.integration.CacheLoader;
import javax.cache.integration.CacheLoaderException;
import javax.cache.integration.CompletionListener;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.EntryProcessorResult;

import net.helenus.core.cache.CacheUtil;
import net.helenus.core.cache.Facet;
import net.helenus.core.cache.MapCache;
import net.helenus.core.operation.AbstractOperation;
import net.helenus.core.operation.BatchOperation;
import net.helenus.mapping.MappingUtil;
import net.helenus.support.CheckedRunnable;
import net.helenus.support.Either;
import net.helenus.support.HelenusException;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Encapsulates the concept of a "transaction" as a unit-of-work. */
public class UnitOfWork implements AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(UnitOfWork.class);

  public final UnitOfWork parent;
  private final List<UnitOfWork> nested = new ArrayList<>();
  private final Table<String, String, Either<Object, List<Facet>>> cache = HashBasedTable.create();
  private final EvictTrackingMapCache<String, Object> statementCache;
  protected final HelenusSession session;
  protected String purpose;
  protected List<String> nestedPurposes = new ArrayList<String>();
  protected String info;
  protected int cacheHits = 0;
  protected int cacheMisses = 0;
  protected int databaseLookups = 0;
  protected final Stopwatch elapsedTime;
  protected Map<String, Double> databaseTime = new HashMap<>();
  protected double cacheLookupTimeMSecs = 0.0;
  private List<CheckedRunnable> commitThunks = new ArrayList<>();
  private List<CheckedRunnable> abortThunks = new ArrayList<>();
  private Consumer<? super Throwable> exceptionallyThunk;
  private List<CompletableFuture<?>> asyncOperationFutures = new ArrayList<CompletableFuture<?>>();
  private boolean aborted = false;
  private boolean committed = false;
  private long committedAt = 0L;
  private BatchOperation batch;

  public UnitOfWork(HelenusSession session) {
    this(session, null);
  }

  public UnitOfWork(HelenusSession session, UnitOfWork parent) {
    Objects.requireNonNull(session, "containing session cannot be null");

    this.parent = parent;
    if (parent != null) {
      parent.addNestedUnitOfWork(this);
    }
    this.session = session;
    CacheLoader<String, Object> cacheLoader = null;
    if (parent != null) {
      cacheLoader =
          new CacheLoader<String, Object>() {

            Cache<String, Object> cache = parent.getCache();

            @Override
            public Object load(String key) throws CacheLoaderException {
              return cache.get(key);
            }

            @Override
            public Map<String, Object> loadAll(Iterable<? extends String> keys)
                throws CacheLoaderException {
              Map<String, Object> kvp = new HashMap<String, Object>();
              for (String key : keys) {
                kvp.put(key, cache.get(key));
              }
              return kvp;
            }
          };
    }
    this.elapsedTime = Stopwatch.createUnstarted();
    this.statementCache = new EvictTrackingMapCache<String, Object>(null, "UOW(" + hashCode() + ")", cacheLoader, true);
  }

  public void addDatabaseTime(String name, Stopwatch amount) {
    Double time = databaseTime.get(name);
    if (time == null) {
      databaseTime.put(name, (double) amount.elapsed(TimeUnit.MICROSECONDS));
    } else {
      databaseTime.put(name, time + amount.elapsed(TimeUnit.MICROSECONDS));
    }
  }

  public void addCacheLookupTime(Stopwatch amount) {
    cacheLookupTimeMSecs += amount.elapsed(TimeUnit.MICROSECONDS);
  }

  public void addNestedUnitOfWork(UnitOfWork uow) {
    synchronized (nested) {
      nested.add(uow);
    }
  }

  /**
   * Marks the beginning of a transactional section of work. Will write a
   * recordCacheAndDatabaseOperationCount to the shared write-ahead log.
   *
   * @return the handle used to commit or abort the work.
   */
  public synchronized UnitOfWork begin() {
    elapsedTime.start();
    // log.record(txn::start)
    return this;
  }

  public String getPurpose() {
    return purpose;
  }

  public UnitOfWork setPurpose(String purpose) {
    this.purpose = purpose;
    return this;
  }

  public void addFuture(CompletableFuture<?> future) {
    asyncOperationFutures.add(future);
  }

  public void setInfo(String info) {
    this.info = info;
  }

  public void recordCacheAndDatabaseOperationCount(int cache, int ops) {
    if (cache > 0) {
      cacheHits += cache;
    } else {
      cacheMisses += Math.abs(cache);
    }
    if (ops > 0) {
      databaseLookups += ops;
    }
  }

  public String logTimers(String what) {
    double e = (double) elapsedTime.elapsed(TimeUnit.MICROSECONDS) / 1000.0;
    double d = 0.0;
    double c = cacheLookupTimeMSecs / 1000.0;
    double fc = (c / e) * 100.0;
    String database = "";
    if (databaseTime.size() > 0) {
      List<String> dbt = new ArrayList<>(databaseTime.size());
      for (Map.Entry<String, Double> dt : databaseTime.entrySet()) {
        double t = dt.getValue() / 1000.0;
        d += t;
        dbt.add(String.format("%s took %,.3fms %,2.2f%%", dt.getKey(), t, (t / e) * 100.0));
      }
      double fd = (d / e) * 100.0;
      database =
          String.format(
              ", %d quer%s (%,.3fms %,2.2f%% - %s)",
              databaseLookups, (databaseLookups > 1) ? "ies" : "y", d, fd, String.join(", ", dbt));
    }
    String cache = "";
    if (cacheLookupTimeMSecs > 0) {
      int cacheLookups = cacheHits + cacheMisses;
      cache =
          String.format(
              " with %d cache lookup%s (%,.3fms %,2.2f%% - %,d hit, %,d miss)",
              cacheLookups, cacheLookups > 1 ? "s" : "", c, fc, cacheHits, cacheMisses);
    }
    String da = "";
    if (databaseTime.size() > 0 || cacheLookupTimeMSecs > 0) {
      double dat = d + c;
      double daf = (dat / e) * 100;
      da =
          String.format(
              " consuming %,.3fms for data access, or %,2.2f%% of total UOW time.", dat, daf);
    }
    String x = nestedPurposes.stream().distinct().collect(Collectors.joining(", "));
    String n =
        nested
            .stream()
            .map(uow -> String.valueOf(uow.hashCode()))
            .collect(Collectors.joining(", "));
    String s =
        String.format(
            Locale.US,
            "UOW(%s%s) %s in %,.3fms%s%s%s%s%s%s",
            hashCode(),
            (nested.size() > 0 ? ", [" + n + "]" : ""),
            what,
            e,
            cache,
            database,
            da,
            (purpose == null ? "" : " " + purpose),
            (nestedPurposes.isEmpty()) ? "" : ", " + x,
            (info == null) ? "" : " " + info);
    return s;
  }

  private void applyPostCommitFunctions(String what, List<CheckedRunnable> thunks, Consumer<? super Throwable> exceptionallyThunk) {
    if (!thunks.isEmpty()) {
      for (CheckedRunnable f : thunks) {
          try {
              f.run();
          } catch (Throwable t) {
              if (exceptionallyThunk != null) {
                  exceptionallyThunk.accept(t);
              }
          }
      }
    }
  }

  public Optional<Object> cacheLookup(List<Facet> facets) {
    String tableName = CacheUtil.schemaName(facets);
    Optional<Object> result = Optional.empty();
    for (Facet facet : facets) {
      if (!facet.fixed()) {
        String columnName = facet.name() + "==" + facet.value();
        Either<Object, List<Facet>> eitherValue = cache.get(tableName, columnName);
        if (eitherValue != null) {
          Object value = deleted;
          if (eitherValue.isLeft()) {
            value = eitherValue.getLeft();
          }
          return Optional.of(value);
        }
      }
    }

    // Be sure to check all enclosing UnitOfWork caches as well, we may be nested.
    result = checkParentCache(facets);
    if (result.isPresent()) {
      Object r = result.get();
      Class<?> iface = MappingUtil.getMappingInterface(r);
      if (Helenus.entity(iface).isDraftable()) {
        cacheUpdate(r, facets);
      } else {
        cacheUpdate(SerializationUtils.<Serializable>clone((Serializable) r), facets);
      }
    }
    return result;
  }

  private Optional<Object> checkParentCache(List<Facet> facets) {
    Optional<Object> result = Optional.empty();
    if (parent != null) {
      result = parent.checkParentCache(facets);
    }
    return result;
  }

  public List<Facet> cacheEvict(List<Facet> facets) {
    Either<Object, List<Facet>> deletedObjectFacets = Either.right(facets);
    String tableName = CacheUtil.schemaName(facets);
    Optional<Object> optionalValue = cacheLookup(facets);

    for (Facet facet : facets) {
      if (!facet.fixed()) {
        String columnKey = facet.name() + "==" + facet.value();
        // mark the value identified by the facet to `deleted`
        cache.put(tableName, columnKey, deletedObjectFacets);
      }
    }

    // Now, look for other row/col pairs that referenced the same object, mark them
    // `deleted` if the cache had a value before we added the deleted marker objects.
    if (optionalValue.isPresent()) {
      Object value = optionalValue.get();
      cache
          .columnKeySet()
          .forEach(
              columnKey -> {
                Either<Object, List<Facet>> eitherCachedValue = cache.get(tableName, columnKey);
                if (eitherCachedValue.isLeft()) {
                  Object cachedValue = eitherCachedValue.getLeft();
                  if (cachedValue == value) {
                    cache.put(tableName, columnKey, deletedObjectFacets);
                    String[] parts = columnKey.split("==");
                    facets.add(new Facet<String>(parts[0], parts[1]));
                  }
                }
              });
    }
    return facets;
  }

  public Cache<String, Object> getCache() {
    return statementCache;
  }

  public Object cacheUpdate(Object value, List<Facet> facets) {
    Object result = null;
    String tableName = CacheUtil.schemaName(facets);
    for (Facet facet : facets) {
      if (!facet.fixed()) {
        if (facet.alone()) {
          String columnName = facet.name() + "==" + facet.value();
          if (result == null) result = cache.get(tableName, columnName);
          cache.put(tableName, columnName, Either.left(value));
        }
      }
    }
    return result;
  }

  public void batch(AbstractOperation s) {
    if (batch == null) {
      batch = new BatchOperation(session);
    }
    batch.add(s);
  }

  private Iterator<UnitOfWork> getChildNodes() {
    return nested.iterator();
  }

  /**
   * Checks to see if the work performed between calling begin and now can be committed or not.
   *
   * @return a function from which to chain work that only happens when commit is successful
   * @throws HelenusException when the work overlaps with other concurrent writers.
   */
  public synchronized PostCommitFunction<Void, Void> commit() throws HelenusException {

    if (isDone()) {
      return PostCommitFunction.NULL_ABORT;
    }

    // Only the outer-most UOW batches statements for commit time, execute them.
    if (batch != null) {
      committedAt = batch.sync(this); //TODO(gburd): update cache with writeTime...
    }

    // All nested UnitOfWork should be committed (not aborted) before calls to
    // commit, check.
    boolean canCommit = true;
    TreeTraverser<UnitOfWork> traverser = TreeTraverser.using(node -> node::getChildNodes);
    for (UnitOfWork uow : traverser.postOrderTraversal(this)) {
      if (this != uow) {
        canCommit &= (!uow.aborted && uow.committed);
      }
    }

    if (!canCommit) {

      if (parent == null) {

        // Apply all post-commit abort functions, this is the outer-most UnitOfWork.
        traverser
            .postOrderTraversal(this)
            .forEach(
                uow -> {
                  applyPostCommitFunctions("aborted", abortThunks, exceptionallyThunk);
                });

        elapsedTime.stop();
        if (LOG.isInfoEnabled()) {
          LOG.info(logTimers("aborted"));
        }
      }

      return PostCommitFunction.NULL_ABORT;
    } else {
      committed = true;
      aborted = false;

      if (parent == null) {

        // Apply all post-commit commit functions, this is the outer-most UnitOfWork.
        traverser
            .postOrderTraversal(this)
            .forEach(
                uow -> {
                  applyPostCommitFunctions("committed", uow.commitThunks, exceptionallyThunk);
                });

        // Merge our statement cache into the session cache if it exists.
        CacheManager cacheManager = session.getCacheManager();
        if (cacheManager != null) {
          for (Map.Entry<String, Object> entry :
              (Set<Map.Entry<String, Object>>) statementCache.<Map>unwrap(Map.class).entrySet()) {
            String[] keyParts = entry.getKey().split("\\.");
            if (keyParts.length == 2) {
              String cacheName = keyParts[0];
              String key = keyParts[1];
              if (!StringUtils.isBlank(cacheName) && !StringUtils.isBlank(key)) {
                Cache<Object, Object> cache = cacheManager.getCache(cacheName);
                if (cache != null) {
                  Object value = entry.getValue();
                  if (value == deleted) {
                    cache.remove(key);
                  } else {
                    cache.put(key.toString(), value);
                  }
                }
              }
            }
          }
        }

        // Merge our cache into the session cache.
        session.mergeCache(cache);

        // Spoil any lingering futures that may be out there.
        asyncOperationFutures.forEach(
            f ->
                f.completeExceptionally(
                    new HelenusException(
                        "Futures must be resolved before their unit of work has committed/aborted.")));

        elapsedTime.stop();
        if (LOG.isInfoEnabled()) {
          LOG.info(logTimers("committed"));
        }

        return PostCommitFunction.NULL_COMMIT;
      } else {
        // Merge cache and statistics into parent if there is one.
        parent.statementCache.putAll(statementCache.<Map>unwrap(Map.class));
        parent.statementCache.removeAll(statementCache.getDeletions());
        parent.mergeCache(cache);
        parent.addBatched(batch);
        if (purpose != null) {
          parent.nestedPurposes.add(purpose);
        }
        parent.cacheHits += cacheHits;
        parent.cacheMisses += cacheMisses;
        parent.databaseLookups += databaseLookups;
        parent.cacheLookupTimeMSecs += cacheLookupTimeMSecs;
        for (Map.Entry<String, Double> dt : databaseTime.entrySet()) {
          String name = dt.getKey();
          if (parent.databaseTime.containsKey(name)) {
            double t = parent.databaseTime.get(name);
            parent.databaseTime.put(name, t + dt.getValue());
          } else {
            parent.databaseTime.put(name, dt.getValue());
          }
        }
      }
    }
    // TODO(gburd): hopefully we'll be able to detect conflicts here and so we'd want to...
    // else {
    // Constructor<T> ctor = clazz.getConstructor(conflictExceptionClass);
    // T object = ctor.newInstance(new Object[] { String message });
    // }
    return new PostCommitFunction<Void, Void>(commitThunks, abortThunks, exceptionallyThunk, true);
  }

  private void addBatched(BatchOperation batchArg) {
    if (batchArg != null) {
      if (this.batch == null) {
        this.batch = batchArg;
      } else {
        this.batch.addAll(batchArg);
      }
    }
  }

  /**
   * Explicitly abort the work within this unit of work. Any nested aborted unit of work will
   * trigger the entire unit of work to commit.
   */
  public synchronized void abort() {
    if (!aborted) {
      aborted = true;

      // Spoil any pending futures created within the context of this unit of work.
      asyncOperationFutures.forEach(
          f ->
              f.completeExceptionally(
                  new HelenusException(
                      "Futures must be resolved before their unit of work has committed/aborted.")));

      TreeTraverser<UnitOfWork> traverser = TreeTraverser.using(node -> node::getChildNodes);
      traverser
          .postOrderTraversal(this)
          .forEach(
              uow -> {
                applyPostCommitFunctions("aborted", uow.abortThunks, exceptionallyThunk);
                uow.abortThunks.clear();
              });

      if (parent == null) {
        elapsedTime.stop();
        if (LOG.isInfoEnabled()) {
          LOG.info(logTimers("aborted"));
        }
      }

      // TODO(gburd): when we integrate the transaction support we'll need to...
      // log.record(txn::abort)
      // cache.invalidateSince(txn::start time)
    }
  }

  private void mergeCache(Table<String, String, Either<Object, List<Facet>>> from) {
    Table<String, String, Either<Object, List<Facet>>> to = this.cache;
    from.rowMap()
        .forEach(
            (rowKey, columnMap) -> {
              columnMap.forEach(
                  (columnKey, value) -> {
                    if (to.contains(rowKey, columnKey)) {
                      to.put(
                          rowKey,
                          columnKey,
                          Either.left(
                              CacheUtil.merge(
                                  to.get(rowKey, columnKey).getLeft(),
                                  from.get(rowKey, columnKey).getLeft())));
                    } else {
                      to.put(rowKey, columnKey, from.get(rowKey, columnKey));
                    }
                  });
            });
  }

  public boolean isDone() {
    return aborted || committed;
  }

  public String describeConflicts() {
    return "it's complex...";
  }

  @Override
  public void close() throws HelenusException {
    // Closing a UnitOfWork will abort iff we've not already aborted or committed this unit of work.
    if (aborted == false && committed == false) {
      abort();
    }
  }

  public boolean hasAborted() {
    return aborted;
  }

  public boolean hasCommitted() {
    return committed;
  }

  public long committedAt() {
    return committedAt;
  }

 private static class EvictTrackingMapCache<K, V> implements Cache<K, V> {
      private final Set<K> deletes;
      private final Cache<K, V> delegate;

      public EvictTrackingMapCache(CacheManager manager, String name, CacheLoader<K, V> cacheLoader,
              boolean isReadThrough) {
          deletes = Collections.synchronizedSet(new HashSet<>());
          delegate = new MapCache<>(manager, name, cacheLoader, isReadThrough);
      }

      /** Non-interface method; should only be called by UnitOfWork when merging to an enclosing UnitOfWork. */
      public Set<K> getDeletions() {
          return new HashSet<>(deletes);
      }

      @Override
      public V get(K key) {
          if (deletes.contains(key)) {
              return null;
          }

          return delegate.get(key);
      }

      @Override
      public Map<K, V> getAll(Set<? extends K> keys) {
          Set<? extends K> clonedKeys = new HashSet<>(keys);
          clonedKeys.removeAll(deletes);
          return delegate.getAll(clonedKeys);
      }

      @Override
      public boolean containsKey(K key) {
          if (deletes.contains(key)) {
              return false;
          }

          return delegate.containsKey(key);
      }

      @Override
      public void loadAll(Set<? extends K> keys, boolean replaceExistingValues, CompletionListener listener) {
          Set<? extends K> clonedKeys = new HashSet<>(keys);
          clonedKeys.removeAll(deletes);
          delegate.loadAll(clonedKeys, replaceExistingValues, listener);
      }

      @Override
      public void put(K key, V value) {
          if (deletes.contains(key)) {
              deletes.remove(key);
          }

          delegate.put(key, value);
      }

      @Override
      public V getAndPut(K key, V value) {
          if (deletes.contains(key)) {
              deletes.remove(key);
          }

          return delegate.getAndPut(key, value);
      }

      @Override
      public void putAll(Map<? extends K, ? extends V> map) {
          deletes.removeAll(map.keySet());
          delegate.putAll(map);
      }

      @Override
      public synchronized boolean putIfAbsent(K key, V value) {
          if (!delegate.containsKey(key) && deletes.contains(key)) {
              deletes.remove(key);
          }

          return delegate.putIfAbsent(key,  value);
      }

      @Override
      public boolean remove(K key) {
          boolean removed = delegate.remove(key);
          deletes.add(key);
          return removed;
      }

      @Override
      public boolean remove(K key, V value) {
          boolean removed = delegate.remove(key, value);
          if (removed) {
              deletes.add(key);
          }

          return removed;
      }

      @Override
      public V getAndRemove(K key) {
          V value = delegate.getAndRemove(key);
          deletes.add(key);
          return value;
      }

      @Override
      public void removeAll(Set<? extends K> keys) {
          Set<? extends K> cloneKeys = new HashSet<>(keys);
          delegate.removeAll(cloneKeys);
          deletes.addAll(cloneKeys);
      }

      @Override
      @SuppressWarnings("unchecked")
      public synchronized void removeAll() {
          Map<K, V> impl = delegate.unwrap(Map.class);
          Set<K> keys = impl.keySet();
          delegate.removeAll();
          deletes.addAll(keys);
      }

      @Override
      public void clear() {
          delegate.clear();
          deletes.clear();
      }

    @Override
    public boolean replace(K key, V oldValue, V newValue) {
        if (deletes.contains(key)) {
            return false;
        }

        return delegate.replace(key, oldValue, newValue);
    }

    @Override
    public boolean replace(K key, V value) {
        if (deletes.contains(key)) {
            return false;
        }

        return delegate.replace(key, value);
    }

    @Override
    public V getAndReplace(K key, V value) {
        if (deletes.contains(key)) {
            return null;
        }

        return delegate.getAndReplace(key, value);
    }

    @Override
    public <C extends Configuration<K, V>> C getConfiguration(Class<C> clazz) {
        return delegate.getConfiguration(clazz);
    }

    @Override
    public <T> T invoke(K key, EntryProcessor<K, V, T> processor, Object... arguments)
            throws EntryProcessorException {
        if (deletes.contains(key)) {
            return null;
        }

        return delegate.invoke(key,  processor, arguments);
    }

    @Override
    public <T> Map<K, EntryProcessorResult<T>> invokeAll(Set<? extends K> keys, EntryProcessor<K, V, T> processor,
            Object... arguments) {
        Set<? extends K> clonedKeys = new HashSet<>(keys);
        clonedKeys.removeAll(deletes);
        return delegate.invokeAll(clonedKeys, processor, arguments);
    }

    @Override
    public String getName() {
        return delegate.getName();
    }

    @Override
    public CacheManager getCacheManager() {
        return delegate.getCacheManager();
    }

    @Override
    public void close() {
        delegate.close();
    }

    @Override
    public boolean isClosed() {
        return delegate.isClosed();
    }

    @Override
    public <T> T unwrap(Class<T> clazz) {
        return delegate.unwrap(clazz);
    }

    @Override
    public void registerCacheEntryListener(CacheEntryListenerConfiguration<K, V> cacheEntryListenerConfiguration) {
        delegate.registerCacheEntryListener(cacheEntryListenerConfiguration);
    }

    @Override
    public void deregisterCacheEntryListener(CacheEntryListenerConfiguration<K, V> cacheEntryListenerConfiguration) {
        delegate.deregisterCacheEntryListener(cacheEntryListenerConfiguration);
    }

    @Override
    public Iterator<Entry<K, V>> iterator() {
        return delegate.iterator();
    }
  }
}
