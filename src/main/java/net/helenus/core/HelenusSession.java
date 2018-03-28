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

import static net.helenus.core.Query.eq;

import com.codahale.metrics.MetricRegistry;
import com.datastax.driver.core.*;
import com.google.common.collect.Table;
import java.io.Closeable;
import java.io.PrintStream;
import java.util.*;
import java.util.concurrent.Executor;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.cache.Cache;
import javax.cache.CacheManager;
import net.helenus.core.cache.CacheUtil;
import net.helenus.core.cache.Facet;
import net.helenus.core.cache.UnboundFacet;
import net.helenus.core.operation.*;
import net.helenus.core.reflect.Drafted;
import net.helenus.core.reflect.HelenusPropertyNode;
import net.helenus.core.reflect.MapExportable;
import net.helenus.mapping.HelenusEntity;
import net.helenus.mapping.HelenusProperty;
import net.helenus.mapping.MappingUtil;
import net.helenus.mapping.value.*;
import net.helenus.support.*;
import net.helenus.support.Fun.Tuple1;
import net.helenus.support.Fun.Tuple2;
import net.helenus.support.Fun.Tuple6;

public class HelenusSession extends AbstractSessionOperations implements Closeable {
  public static final Object deleted = new Object();

  private final Session session;
  private final CodecRegistry registry;
  private final ConsistencyLevel defaultConsistencyLevel;
  private final boolean defaultQueryIdempotency;
  private final MetricRegistry metricRegistry;
  private final PrintStream printStream;
  private final SessionRepository sessionRepository;
  private final Executor executor;
  private final boolean dropSchemaOnClose;
  private final CacheManager cacheManager;
  private final RowColumnValueProvider valueProvider;
  private final StatementColumnValuePreparer valuePreparer;
  private final Metadata metadata;
  private volatile String usingKeyspace;
  private volatile boolean showCql;
  private volatile boolean showValues;

  HelenusSession(
      Session session,
      String usingKeyspace,
      CodecRegistry registry,
      boolean showCql,
      boolean showValues,
      PrintStream printStream,
      SessionRepositoryBuilder sessionRepositoryBuilder,
      Executor executor,
      boolean dropSchemaOnClose,
      ConsistencyLevel consistencyLevel,
      boolean defaultQueryIdempotency,
      CacheManager cacheManager,
      MetricRegistry metricRegistry) {
    this.session = session;
    this.registry = registry == null ? CodecRegistry.DEFAULT_INSTANCE : registry;
    this.usingKeyspace =
        Objects.requireNonNull(
            usingKeyspace, "keyspace needs to be selected before creating session");
    this.showCql = showCql;
    this.showValues = showValues;
    this.printStream = printStream;
    this.sessionRepository =
        sessionRepositoryBuilder == null ? null : sessionRepositoryBuilder.build();
    this.executor = executor;
    this.dropSchemaOnClose = dropSchemaOnClose;
    this.defaultConsistencyLevel = consistencyLevel;
    this.defaultQueryIdempotency = defaultQueryIdempotency;
    this.metricRegistry = metricRegistry;
    this.cacheManager = cacheManager;

    this.valueProvider = new RowColumnValueProvider(this.sessionRepository);
    this.valuePreparer = new StatementColumnValuePreparer(this.sessionRepository);
    this.metadata = session == null ? null : session.getCluster().getMetadata();
  }

  public UnitOfWork begin() {
    return new UnitOfWork(this).begin();
  }

  public UnitOfWork begin(UnitOfWork parent) {
    return new UnitOfWork(this, parent).begin();
  }

  @Override
  public Session currentSession() {
    return session;
  }

  @Override
  public String usingKeyspace() {
    return usingKeyspace;
  }

  public HelenusSession useKeyspace(String keyspace) {
    session.execute(SchemaUtil.use(keyspace, false));
    this.usingKeyspace = keyspace;
    return this;
  }

  @Override
  public boolean isShowCql() {
    return showCql;
  }

  @Override
  public PrintStream getPrintStream() {
    return printStream;
  }

  public HelenusSession showCql() {
    this.showCql = true;
    return this;
  }

  public HelenusSession showCql(boolean showCql) {
    this.showCql = showCql;
    return this;
  }

  public HelenusSession showQueryValuesInLog(boolean showValues) {
    this.showValues = showValues;
    return this;
  }

  public HelenusSession showQueryValuesInLog() {
    this.showValues = true;
    return this;
  }

  public boolean showValues() {
    return showValues;
  }

  @Override
  public Executor getExecutor() {
    return executor;
  }

  @Override
  public SessionRepository getSessionRepository() {
    return sessionRepository;
  }

  @Override
  public ColumnValueProvider getValueProvider() {
    return valueProvider;
  }

  @Override
  public ColumnValuePreparer getValuePreparer() {
    return valuePreparer;
  }

  @Override
  public MetricRegistry getMetricRegistry() {
    return metricRegistry;
  }

  @Override
  public ConsistencyLevel getDefaultConsistencyLevel() {
    return defaultConsistencyLevel;
  }

  @Override
  public boolean getDefaultQueryIdempotency() {
    return defaultQueryIdempotency;
  }

  @Override
  public Object checkCache(String tableName, List<Facet> facets) {
    Object result = null;
    if (cacheManager != null) {
      Cache<String, Object> cache = cacheManager.getCache(tableName);
      if (cache != null) {
        for (String key : CacheUtil.flatKeys(tableName, facets)) {
          result = cache.get(key);
          if (result != null) {
            return result;
          }
        }
      }
    }
    return null;
  }

  @Override
  public void cacheEvict(List<Facet> facets) {
    if (cacheManager != null) {
      String tableName = CacheUtil.schemaName(facets);
      Cache<String, Object> cache = cacheManager.getCache(tableName);
      if (cache != null) {
        CacheUtil.flatKeys(tableName, facets).forEach(key -> cache.remove(key));
      }
    }
  }

  @Override
  public void updateCache(Object pojo, List<Facet> facets) {
    Map<String, Object> valueMap =
        pojo instanceof MapExportable ? ((MapExportable) pojo).toMap() : null;
    List<Facet> boundFacets = new ArrayList<>();
    for (Facet facet : facets) {
      if (facet instanceof UnboundFacet) {
        UnboundFacet unboundFacet = (UnboundFacet) facet;
        UnboundFacet.Binder binder = unboundFacet.binder();
        for (HelenusProperty prop : unboundFacet.getProperties()) {
          Object value;
          if (valueMap == null) {
            value = BeanColumnValueProvider.INSTANCE.getColumnValue(pojo, -1, prop, false);
            if (value != null) {
              binder.setValueForProperty(prop, value.toString());
            }
          } else {
            value = valueMap.get(prop.getPropertyName());
            if (value != null) binder.setValueForProperty(prop, value.toString());
          }
        }
        if (binder.isBound()) {
          boundFacets.add(binder.bind());
        }
      } else {
        boundFacets.add(facet);
      }
    }
    String tableName = CacheUtil.schemaName(facets);
    List<String[]> facetCombinations = CacheUtil.flattenFacets(boundFacets);
    replaceCachedFacetValues(pojo, tableName, facetCombinations);
  }

  @Override
  public void mergeCache(Table<String, String, Either<Object, List<Facet>>> uowCache) {
    if (cacheManager != null) {
      List<Object> items =
          uowCache
              .values()
              .stream()
              .filter(Either::isLeft)
              .map(Either::getLeft)
              .distinct()
              .collect(Collectors.toList());
      for (Object pojo : items) {
        HelenusEntity entity = Helenus.resolve(MappingUtil.getMappingInterface(pojo));
        Map<String, Object> valueMap =
            pojo instanceof MapExportable ? ((MapExportable) pojo).toMap() : null;
        if (entity.isCacheable()) {
          List<Facet> boundFacets = new ArrayList<>();
          for (Facet facet : entity.getFacets()) {
            if (facet instanceof UnboundFacet) {
              UnboundFacet unboundFacet = (UnboundFacet) facet;
              UnboundFacet.Binder binder = unboundFacet.binder();
              unboundFacet
                  .getProperties()
                  .forEach(
                      prop -> {
                        if (valueMap == null) {
                          Object value =
                              BeanColumnValueProvider.INSTANCE.getColumnValue(pojo, -1, prop);
                          binder.setValueForProperty(prop, value.toString());
                        } else {
                          Object v = valueMap.get(prop.getPropertyName());
                          if (v != null) {
                            binder.setValueForProperty(prop, v.toString());
                          }
                        }
                      });
              if (binder.isBound()) {
                boundFacets.add(binder.bind());
              }
            } else {
              boundFacets.add(facet);
            }
          }
          List<String[]> facetCombinations = CacheUtil.flattenFacets(boundFacets);
          String tableName = CacheUtil.schemaName(boundFacets);
          replaceCachedFacetValues(pojo, tableName, facetCombinations);
        }
      }

      List<List<Facet>> deletedFacetSets =
          uowCache
              .values()
              .stream()
              .filter(Either::isRight)
              .map(Either::getRight)
              .collect(Collectors.toList());
      for (List<Facet> facets : deletedFacetSets) {
        String tableName = CacheUtil.schemaName(facets);
        Cache<String, Object> cache = cacheManager.getCache(tableName);
        if (cache != null) {
          List<String> keys = CacheUtil.flatKeys(tableName, facets);
          keys.forEach(key -> cache.remove(key));
        }
      }
    }
  }

  private void replaceCachedFacetValues(
      Object pojo, String tableName, List<String[]> facetCombinations) {
    if (cacheManager != null) {
      for (String[] combination : facetCombinations) {
        String cacheKey = tableName + "." + Arrays.toString(combination);
        Cache<String, Object> cache = cacheManager.getCache(tableName);
        if (cache != null) {
          if (pojo == null || pojo == HelenusSession.deleted) {
            cache.remove(cacheKey);
          } else {
            cache.put(cacheKey, pojo);
          }
        }
      }
    }
  }

  public CacheManager getCacheManager() {
    return cacheManager;
  }

  public Metadata getMetadata() {
    return metadata;
  }

  public <E> SelectOperation<E> select(E pojo) {
    Objects.requireNonNull(
        pojo, "supplied object must be a dsl for a registered entity but cannot be null");
    ColumnValueProvider valueProvider = getValueProvider();
    HelenusEntity entity = Helenus.resolve(pojo);
    Class<?> entityClass = entity.getMappingInterface();

    return new SelectOperation<E>(
        this,
        entity,
        (r) -> {
          Map<String, Object> map = new ValueProviderMap(r, valueProvider, entity);
          return (E) Helenus.map(entityClass, map);
        });
  }

  public <E> SelectOperation<E> select(Class<E> entityClass) {
    Objects.requireNonNull(entityClass, "entityClass is empty");
    ColumnValueProvider valueProvider = getValueProvider();
    HelenusEntity entity = Helenus.entity(entityClass);

    return new SelectOperation<E>(
        this,
        entity,
        (r) -> {
          Map<String, Object> map = new ValueProviderMap(r, valueProvider, entity);
          return (E) Helenus.map(entityClass, map);
        });
  }

  public SelectOperation<Fun.ArrayTuple> select() {
    return new SelectOperation<Fun.ArrayTuple>(this);
  }

  public <E> SelectOperation<E> selectAll(Class<E> entityClass) {
    Objects.requireNonNull(entityClass, "entityClass is empty");
    HelenusEntity entity = Helenus.entity(entityClass);

    return new SelectOperation<E>(
        this,
        entity,
        (r) -> {
          Map<String, Object> map = new ValueProviderMap(r, valueProvider, entity);
          return (E) Helenus.map(entityClass, map);
        });
  }

  public <E> SelectOperation<Row> selectAll(E pojo) {
    Objects.requireNonNull(
        pojo, "supplied object must be a dsl for a registered entity but cannot be null");
    HelenusEntity entity = Helenus.resolve(pojo);
    return new SelectOperation<Row>(this, entity);
  }

  public <E> SelectOperation<E> selectAll(Class<E> entityClass, Function<Row, E> rowMapper) {
    Objects.requireNonNull(entityClass, "entityClass is empty");
    Objects.requireNonNull(rowMapper, "rowMapper is empty");
    return new SelectOperation<E>(this, Helenus.entity(entityClass), rowMapper);
  }

  public <V1> SelectOperation<Fun.Tuple1<V1>> select(Getter<V1> getter1) {
    Objects.requireNonNull(getter1, "field 1 is empty");

    HelenusPropertyNode p1 = MappingUtil.resolveMappingProperty(getter1);
    return new SelectOperation<Tuple1<V1>>(
        this, new Mappers.Mapper1<V1>(getValueProvider(), p1), p1);
  }

  public <V1, V2> SelectOperation<Tuple2<V1, V2>> select(Getter<V1> getter1, Getter<V2> getter2) {
    Objects.requireNonNull(getter1, "field 1 is empty");
    Objects.requireNonNull(getter2, "field 2 is empty");

    HelenusPropertyNode p1 = MappingUtil.resolveMappingProperty(getter1);
    HelenusPropertyNode p2 = MappingUtil.resolveMappingProperty(getter2);
    return new SelectOperation<Fun.Tuple2<V1, V2>>(
        this, new Mappers.Mapper2<V1, V2>(getValueProvider(), p1, p2), p1, p2);
  }

  public <V1, V2, V3> SelectOperation<Fun.Tuple3<V1, V2, V3>> select(
      Getter<V1> getter1, Getter<V2> getter2, Getter<V3> getter3) {
    Objects.requireNonNull(getter1, "field 1 is empty");
    Objects.requireNonNull(getter2, "field 2 is empty");
    Objects.requireNonNull(getter3, "field 3 is empty");

    HelenusPropertyNode p1 = MappingUtil.resolveMappingProperty(getter1);
    HelenusPropertyNode p2 = MappingUtil.resolveMappingProperty(getter2);
    HelenusPropertyNode p3 = MappingUtil.resolveMappingProperty(getter3);
    return new SelectOperation<Fun.Tuple3<V1, V2, V3>>(
        this, new Mappers.Mapper3<V1, V2, V3>(getValueProvider(), p1, p2, p3), p1, p2, p3);
  }

  public <V1, V2, V3, V4> SelectOperation<Fun.Tuple4<V1, V2, V3, V4>> select(
      Getter<V1> getter1, Getter<V2> getter2, Getter<V3> getter3, Getter<V4> getter4) {
    Objects.requireNonNull(getter1, "field 1 is empty");
    Objects.requireNonNull(getter2, "field 2 is empty");
    Objects.requireNonNull(getter3, "field 3 is empty");
    Objects.requireNonNull(getter4, "field 4 is empty");

    HelenusPropertyNode p1 = MappingUtil.resolveMappingProperty(getter1);
    HelenusPropertyNode p2 = MappingUtil.resolveMappingProperty(getter2);
    HelenusPropertyNode p3 = MappingUtil.resolveMappingProperty(getter3);
    HelenusPropertyNode p4 = MappingUtil.resolveMappingProperty(getter4);
    return new SelectOperation<Fun.Tuple4<V1, V2, V3, V4>>(
        this,
        new Mappers.Mapper4<V1, V2, V3, V4>(getValueProvider(), p1, p2, p3, p4),
        p1,
        p2,
        p3,
        p4);
  }

  public <V1, V2, V3, V4, V5> SelectOperation<Fun.Tuple5<V1, V2, V3, V4, V5>> select(
      Getter<V1> getter1,
      Getter<V2> getter2,
      Getter<V3> getter3,
      Getter<V4> getter4,
      Getter<V5> getter5) {
    Objects.requireNonNull(getter1, "field 1 is empty");
    Objects.requireNonNull(getter2, "field 2 is empty");
    Objects.requireNonNull(getter3, "field 3 is empty");
    Objects.requireNonNull(getter4, "field 4 is empty");
    Objects.requireNonNull(getter5, "field 5 is empty");

    HelenusPropertyNode p1 = MappingUtil.resolveMappingProperty(getter1);
    HelenusPropertyNode p2 = MappingUtil.resolveMappingProperty(getter2);
    HelenusPropertyNode p3 = MappingUtil.resolveMappingProperty(getter3);
    HelenusPropertyNode p4 = MappingUtil.resolveMappingProperty(getter4);
    HelenusPropertyNode p5 = MappingUtil.resolveMappingProperty(getter5);
    return new SelectOperation<Fun.Tuple5<V1, V2, V3, V4, V5>>(
        this,
        new Mappers.Mapper5<V1, V2, V3, V4, V5>(getValueProvider(), p1, p2, p3, p4, p5),
        p1,
        p2,
        p3,
        p4,
        p5);
  }

  public <V1, V2, V3, V4, V5, V6> SelectOperation<Fun.Tuple6<V1, V2, V3, V4, V5, V6>> select(
      Getter<V1> getter1,
      Getter<V2> getter2,
      Getter<V3> getter3,
      Getter<V4> getter4,
      Getter<V5> getter5,
      Getter<V6> getter6) {
    Objects.requireNonNull(getter1, "field 1 is empty");
    Objects.requireNonNull(getter2, "field 2 is empty");
    Objects.requireNonNull(getter3, "field 3 is empty");
    Objects.requireNonNull(getter4, "field 4 is empty");
    Objects.requireNonNull(getter5, "field 5 is empty");
    Objects.requireNonNull(getter6, "field 6 is empty");

    HelenusPropertyNode p1 = MappingUtil.resolveMappingProperty(getter1);
    HelenusPropertyNode p2 = MappingUtil.resolveMappingProperty(getter2);
    HelenusPropertyNode p3 = MappingUtil.resolveMappingProperty(getter3);
    HelenusPropertyNode p4 = MappingUtil.resolveMappingProperty(getter4);
    HelenusPropertyNode p5 = MappingUtil.resolveMappingProperty(getter5);
    HelenusPropertyNode p6 = MappingUtil.resolveMappingProperty(getter6);
    return new SelectOperation<Tuple6<V1, V2, V3, V4, V5, V6>>(
        this,
        new Mappers.Mapper6<V1, V2, V3, V4, V5, V6>(getValueProvider(), p1, p2, p3, p4, p5, p6),
        p1,
        p2,
        p3,
        p4,
        p5,
        p6);
  }

  public <V1, V2, V3, V4, V5, V6, V7>
      SelectOperation<Fun.Tuple7<V1, V2, V3, V4, V5, V6, V7>> select(
          Getter<V1> getter1,
          Getter<V2> getter2,
          Getter<V3> getter3,
          Getter<V4> getter4,
          Getter<V5> getter5,
          Getter<V6> getter6,
          Getter<V7> getter7) {
    Objects.requireNonNull(getter1, "field 1 is empty");
    Objects.requireNonNull(getter2, "field 2 is empty");
    Objects.requireNonNull(getter3, "field 3 is empty");
    Objects.requireNonNull(getter4, "field 4 is empty");
    Objects.requireNonNull(getter5, "field 5 is empty");
    Objects.requireNonNull(getter6, "field 6 is empty");
    Objects.requireNonNull(getter7, "field 7 is empty");

    HelenusPropertyNode p1 = MappingUtil.resolveMappingProperty(getter1);
    HelenusPropertyNode p2 = MappingUtil.resolveMappingProperty(getter2);
    HelenusPropertyNode p3 = MappingUtil.resolveMappingProperty(getter3);
    HelenusPropertyNode p4 = MappingUtil.resolveMappingProperty(getter4);
    HelenusPropertyNode p5 = MappingUtil.resolveMappingProperty(getter5);
    HelenusPropertyNode p6 = MappingUtil.resolveMappingProperty(getter6);
    HelenusPropertyNode p7 = MappingUtil.resolveMappingProperty(getter7);
    return new SelectOperation<Fun.Tuple7<V1, V2, V3, V4, V5, V6, V7>>(
        this,
        new Mappers.Mapper7<V1, V2, V3, V4, V5, V6, V7>(
            getValueProvider(), p1, p2, p3, p4, p5, p6, p7),
        p1,
        p2,
        p3,
        p4,
        p5,
        p6,
        p7);
  }

  public CountOperation count() {
    return new CountOperation(this);
  }

  public CountOperation count(Object dsl) {
    Objects.requireNonNull(dsl, "dsl is empty");
    return new CountOperation(this, Helenus.resolve(dsl));
  }

  public UpdateOperation<ResultSet> update() {
    return new UpdateOperation<ResultSet>(this);
  }

  public <E> UpdateOperation<E> update(Object pojo) {
    if (pojo instanceof MapExportable == false) {
      throw new HelenusMappingException(
          "update of objects that don't implement MapExportable is not yet supported");
    }
    return new UpdateOperation<E>(this, pojo);
  }

  public <E> UpdateOperation<E> update(Drafted<E> drafted) {
    if (drafted instanceof AbstractEntityDraft == false) {
      throw new HelenusMappingException(
          "update of draft objects that don't inherit from AbstractEntityDraft is not yet supported");
    }
    AbstractEntityDraft<E> draft = (AbstractEntityDraft<E>) drafted;
    UpdateOperation update = new UpdateOperation<E>(this, draft);
    Map<String, Object> map = draft.toMap();
    Set<String> mutatedProperties = draft.mutated();
    HelenusEntity entity = Helenus.entity(draft.getEntityClass());

    // Add all the mutated values contained in the draft.
    entity
        .getOrderedProperties()
        .forEach(
            property -> {
              switch (property.getColumnType()) {
                case PARTITION_KEY:
                case CLUSTERING_COLUMN:
                  break;
                default:
                  String propertyName = property.getPropertyName();
                  if (mutatedProperties.contains(propertyName)) {
                    Object value = map.get(propertyName);
                    Getter<Object> getter =
                        new Getter<Object>() {
                          @Override
                          public Object get() {
                            throw new DslPropertyException(
                                new HelenusPropertyNode(property, Optional.empty()));
                          }
                        };
                    update.set(getter, value);
                  }
              }
            });

    // Add the partition and clustering keys if they were in the draft (normally the
    // case).
    entity
        .getOrderedProperties()
        .forEach(
            property -> {
              switch (property.getColumnType()) {
                case PARTITION_KEY:
                case CLUSTERING_COLUMN:
                  String propertyName = property.getPropertyName();
                  Object value = map.get(propertyName);
                  Getter<Object> getter =
                      new Getter<Object>() {
                        @Override
                        public Object get() {
                          throw new DslPropertyException(
                              new HelenusPropertyNode(property, Optional.empty()));
                        }
                      };
                  update.where(getter, eq(value));
              }
            });

    return update;
  }

  public <V> UpdateOperation<ResultSet> update(Getter<V> getter, V v) {
    Objects.requireNonNull(getter, "field is empty");
    Objects.requireNonNull(v, "value is empty");

    HelenusPropertyNode p = MappingUtil.resolveMappingProperty(getter);

    return new UpdateOperation<ResultSet>(this, p, v);
  }

  public InsertOperation<ResultSet> insert() {
    return new InsertOperation<ResultSet>(this, true);
  }

  public <T> InsertOperation<T> insert(Class<?> resultType) {
    return new InsertOperation<T>(this, resultType, true);
  }

  public <T> InsertOperation<T> insert(T pojo) {
    Objects.requireNonNull(
        pojo,
        "supplied object must be either an instance of the entity class or a dsl for it, but cannot be null");
    HelenusEntity entity = null;
    try {
      entity = Helenus.resolve(pojo);
    } catch (HelenusMappingException e) {
    }
    if (entity != null) {
      return new InsertOperation<T>(this, entity, entity.getMappingInterface(), true);
    } else {
      return this.<T>insert(pojo, null, null);
    }
  }

  public <T> InsertOperation<T> insert(Drafted draft) {
    return insert(draft.build(), draft.mutated(), draft.read());
  }

  private <T> InsertOperation<T> insert(T pojo, Set<String> mutations, Set<String> read) {
    Objects.requireNonNull(pojo, "pojo is empty");

    Class<?> iface = MappingUtil.getMappingInterface(pojo);
    HelenusEntity entity = Helenus.entity(iface);

    return new InsertOperation<T>(this, entity, pojo, mutations, read, true);
  }

  public InsertOperation<ResultSet> upsert() {
    return new InsertOperation<ResultSet>(this, false);
  }

  public <T> InsertOperation<T> upsert(Class<?> resultType) {
    return new InsertOperation<T>(this, resultType, false);
  }

  public <T> InsertOperation<T> upsert(Drafted draft) {
    return this.<T>upsert((T) draft.build(), draft.mutated(), draft.read());
  }

  public <T> InsertOperation<T> upsert(T pojo) {
    Objects.requireNonNull(
        pojo,
        "supplied object must be either an instance of the entity class or a dsl for it, but cannot be null");
    HelenusEntity entity = null;
    try {
      entity = Helenus.resolve(pojo);
    } catch (HelenusMappingException e) {
    }
    if (entity != null) {
      return new InsertOperation<T>(this, entity, entity.getMappingInterface(), false);
    } else {
      return this.<T>upsert(pojo, null, null);
    }
  }

  private <T> InsertOperation<T> upsert(T pojo, Set<String> mutations, Set<String> read) {
    Objects.requireNonNull(pojo, "pojo is empty");

    Class<?> iface = MappingUtil.getMappingInterface(pojo);
    HelenusEntity entity = Helenus.entity(iface);

    return new InsertOperation<T>(this, entity, pojo, mutations, read, false);
  }

  public DeleteOperation delete() {
    return new DeleteOperation(this);
  }

  public DeleteOperation delete(Object dsl) {
    Objects.requireNonNull(dsl, "dsl is empty");
    return new DeleteOperation(this, Helenus.resolve(dsl));
  }

  public Session getSession() {
    return session;
  }

  public <E> E dsl(Class<E> iface) {
    return Helenus.dsl(iface, getMetadata());
  }

  public void close() {
    if (session == null) {
      return;
    }

    if (session.isClosed()) {
      return;
    }

    if (dropSchemaOnClose) {
      dropSchema();
    }

    session.close();
  }

  public CloseFuture closeAsync() {

    if (!session.isClosed() && dropSchemaOnClose) {
      dropSchema();
    }

    return session.closeAsync();
  }

  private void dropSchema() {

    sessionRepository.entities().forEach(e -> dropEntity(e));
  }

  private void dropEntity(HelenusEntity entity) {

    switch (entity.getType()) {
      case TABLE:
        execute(SchemaUtil.dropTable(entity));
        break;

      case UDT:
        execute(SchemaUtil.dropUserType(entity));
        break;

      default:
        throw new HelenusException("Unknown entity type.");
    }
  }
}
