/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.zeppelin.notebook;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class NoteCache extends LinkedHashMap<String, Note> {

  private static final Logger LOGGER = LoggerFactory.getLogger(NoteCache.class);
  /**
   *
   */
  private static final long serialVersionUID = 5219974125499792441L;
  private final int cacheSize;

  public NoteCache(int cacheSize) {
    super(16, 0.75f, true);
    this.cacheSize = cacheSize;
  }

  @Override
  protected boolean removeEldestEntry(Map.Entry<String, Note> eldest) {
    if (size() >= cacheSize) {
      LOGGER.debug("Unloading {}", eldest.getValue().getId());
      eldest.getValue().unLoad();
    }
    return size() >= cacheSize;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = super.hashCode();
    result = prime * result + Objects.hash(cacheSize);
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!super.equals(obj)) {
      return false;
    }
    if (!(obj instanceof NoteCache)) {
      return false;
    }
    NoteCache other = (NoteCache) obj;
    return cacheSize == other.cacheSize;
  }
}
