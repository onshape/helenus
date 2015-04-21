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
package com.noorq.casser.mapping.convert;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

import com.datastax.driver.core.UDTValue;
import com.google.common.collect.ImmutableList;
import com.noorq.casser.core.Casser;
import com.noorq.casser.core.SessionRepository;
import com.noorq.casser.mapping.CasserEntity;
import com.noorq.casser.mapping.value.UDTColumnValueProvider;
import com.noorq.casser.mapping.value.ValueProviderMap;

public final class UDTListToListConverter implements Function<Object, Object> {

	private final Class<?> iface;
	private final CasserEntity entity;
	private final UDTColumnValueProvider valueProvider;
	
	public UDTListToListConverter(Class<?> iface, SessionRepository repository) {
		this.iface = iface;
		this.entity = Casser.entity(iface);
		this.valueProvider = new UDTColumnValueProvider(repository);
	}

	@Override
	public Object apply(Object t) {
		
		List<UDTValue> sourceSet = (List<UDTValue>) t;
		
		ImmutableList.Builder<Object> builder = ImmutableList.builder();
		
		for (UDTValue source : sourceSet) {
		
			Object obj = null;
			
			if (source != null) {
			
				Map<String, Object> map = new ValueProviderMap(source, 
						valueProvider,
						entity);
				
				obj = Casser.map(iface, map);
				
			}
			
			builder.add(obj);
		
		}

		return builder.build();
		
	}

}
