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
package com.noorq.casser.mapping.value;

import java.util.Optional;
import java.util.function.Function;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.TupleType;
import com.datastax.driver.core.querybuilder.BindMarker;
import com.noorq.casser.core.SessionRepository;
import com.noorq.casser.mapping.CasserProperty;

public final class TupleColumnValuePreparer implements ColumnValuePreparer {

	private final TupleType tupleType;
	private final SessionRepository repository;

	public TupleColumnValuePreparer(TupleType tupleType, SessionRepository repository) {
		this.tupleType = tupleType;
		this.repository = repository;
	}
	
	@Override
	public Object prepareColumnValue(Object value, CasserProperty prop) {
		
		if (value != null) {

			if (value instanceof BindMarker) {
				return value;
			}

			Optional<Function<Object, Object>> converter = prop.getWriteConverter(repository);

			if (converter.isPresent()) {
				value = converter.get().apply(value);
			}

			int columnIndex = prop.getOrdinal();
			
			DataType dataType = tupleType.getComponentTypes().get(columnIndex);
			
			return dataType.serialize(value, ProtocolVersion.NEWEST_SUPPORTED);
		}

		return null;
	}
	
}