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

import java.util.Optional;

import com.datastax.driver.core.Metadata;
import net.helenus.core.reflect.HelenusPropertyNode;

public interface DslInstantiator {

    /**
     * Return an instance of either a proxy object or dynamic runtime-generated class that implements the
     * {@code iface} supplied and maps to instances of this type in the database.
     *
     * @param iface
     * @param classLoader
     * @param parent
     * @param metadata
     * @param <E>
     * @return
     */
	<E> E instantiate(Class<E> iface, ClassLoader classLoader, Optional<HelenusPropertyNode> parent, Metadata metadata);

}
