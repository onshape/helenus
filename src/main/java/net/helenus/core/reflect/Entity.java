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
package net.helenus.core.reflect;

import net.helenus.core.Getter;

public interface Entity {
  String WRITTEN_AT_METHOD = "writtenAt";
  String TTL_OF_METHOD = "ttlOf";
  String TOKEN_OF_METHOD = "tokenOf";

    /**
     * The write time for the property in question referenced by the getter.
     *
     * @param getter the property getter
     * @return the timestamp associated with the property identified by the getter
     */
  default Long writtenAt(Getter getter) {
    return 0L;
  }

    /**
     * The write time for the property in question referenced by the property name.
     *
     * @param prop the name of a property in this entity
     * @return the timestamp associated with the property identified by the property name if it exists
     */
  default Long writtenAt(String prop) {
    return 0L;
  };

    /**
     * The time-to-live for the property in question referenced by the getter.
     *
     * @param getter the property getter
     * @return the time-to-live in seconds associated with the property identified by the getter
     */
  default Integer ttlOf(Getter getter) {
    return 0;
  };

    /**
     * The time-to-live for the property in question referenced by the property name.
     *
     * @param prop the name of a property in this entity
     * @return the time-to-live in seconds associated with the property identified by the property name if it exists
     */
  default Integer ttlOf(String prop) {
    return 0;
  };

    /**
     * The token (partition identifier) for this entity which can change over time if
     * the cluster grows or shrinks but should be stable otherwise.
     *
     * @return the token for the entity
     */
  default Long tokenOf() { return 0L; }
}
