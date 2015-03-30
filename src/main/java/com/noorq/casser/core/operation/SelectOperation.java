/*
 *      Copyright (C) 2015 Noorq, Inc.
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
package com.noorq.casser.core.operation;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.querybuilder.BuiltStatement;
import com.datastax.driver.core.querybuilder.Ordering;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.querybuilder.Select.Selection;
import com.datastax.driver.core.querybuilder.Select.Where;
import com.noorq.casser.core.AbstractSessionOperations;
import com.noorq.casser.core.Filter;
import com.noorq.casser.core.Getter;
import com.noorq.casser.core.reflect.CasserPropertyNode;
import com.noorq.casser.mapping.CasserMappingEntity;
import com.noorq.casser.mapping.CasserMappingProperty;
import com.noorq.casser.mapping.MappingUtil;
import com.noorq.casser.mapping.OrderingDirection;
import com.noorq.casser.support.CasserMappingException;


public final class SelectOperation<E> extends AbstractFilterStreamOperation<E, SelectOperation<E>> {

	protected final Function<Row, E> rowMapper;
	protected final CasserPropertyNode[] props;
	
	protected List<Ordering> ordering = null;
	protected Integer limit = null;
	
	public SelectOperation(AbstractSessionOperations sessionOperations, Function<Row, E> rowMapper, CasserPropertyNode... props) {
		super(sessionOperations);
		this.rowMapper = rowMapper;
		this.props = props;	
	}
	
	public CountOperation count() {
		
		CasserMappingEntity entity = null;
		for (CasserPropertyNode prop : props) {
			
			if (entity == null) {
				entity = prop.getEntity();
			}
			else if (entity != prop.getEntity()) {
				throw new CasserMappingException("you can count records only from a single entity " + entity.getMappingInterface() + " or " + prop.getEntity().getMappingInterface());
			}
		}
		
		if (entity == null) {
			throw new CasserMappingException("no entity or table to count data");
		}
		
		return new CountOperation(sessionOps, entity);
	}
	
	public <R> SelectTransformingOperation<R, E> map(Function<E, R> fn) {
		return new SelectTransformingOperation<R, E>(this, fn);
	}
	
	public SelectOperation<E> orderBy(Getter<?> getter, String direction) {
		Objects.requireNonNull(direction, "direction is null");
		return orderBy(getter, OrderingDirection.parseString(direction));
	}
	
	public SelectOperation<E> orderBy(Getter<?> getter, OrderingDirection direction) {
		Objects.requireNonNull(getter, "property is null");
		Objects.requireNonNull(direction, "direction is null");
		
		CasserPropertyNode propNode = MappingUtil.resolveMappingProperty(getter);
		
		if (!propNode.getProperty().isClusteringColumn()) {
			throw new CasserMappingException("property must be a clustering column " + propNode.getProperty().getPropertyName());
		}

		switch(direction) {
			case ASC:
				getOrCreateOrdering().add(QueryBuilder.asc(propNode.getColumnName()));
				return this;
			case DESC:
				getOrCreateOrdering().add(QueryBuilder.desc(propNode.getColumnName()));
				return this;
		}
		
		throw new CasserMappingException("unknown ordering direction " + direction);
	}

	public SelectOperation<E> limit(Integer limit) {
		this.limit = limit;
		return this;
	}
	
	@Override
	public BuiltStatement buildStatement() {
		
		CasserMappingEntity entity = null;
		Selection selection = QueryBuilder.select();
		
		for (CasserPropertyNode prop : props) {
			selection = selection.column(prop.getColumnName());
			
			if (entity == null) {
				entity = prop.getEntity();
			}
			else if (entity != prop.getEntity()) {
				throw new CasserMappingException("you can select columns only from a single entity " + entity.getMappingInterface() + " or " + prop.getEntity().getMappingInterface());
			}
		}
		
		if (entity == null) {
			throw new CasserMappingException("no entity or table to select data");
		}
		
		Select select = selection.from(entity.getName());
		
		if (ordering != null && !ordering.isEmpty()) {
			select.orderBy(ordering.toArray(new Ordering[ordering.size()]));
		}
		
		if (limit != null) {
			select.limit(limit.intValue());
		}
		
		if (filters != null && !filters.isEmpty()) {
		
			Where where = select.where();
			
			for (Filter<?> filter : filters) {
				where.and(filter.getClause(sessionOps.getValuePreparer()));
			}
		}
		
		return select;
	}

	@Override
	public Stream<E> transform(ResultSet resultSet) {
		
		return StreamSupport.stream(
				Spliterators.spliteratorUnknownSize(resultSet.iterator(), Spliterator.ORDERED)
				, false).map(rowMapper);

	}


	private List<Ordering> getOrCreateOrdering() {
		if (ordering == null) {
			ordering = new ArrayList<Ordering>();
		}
		return ordering;
	}
}