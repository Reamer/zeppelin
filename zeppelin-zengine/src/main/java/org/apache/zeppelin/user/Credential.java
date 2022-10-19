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

package org.apache.zeppelin.user;

import java.util.Collections;
import java.util.Optional;
import java.util.Set;

/**
 * Credential POJO
 */
public class Credential extends UsernamePassword {

  private final Set<String> readers;
  private final Set<String> owners;

  public Credential(String username, String password, Set<String> readers, Set<String> owners) {
    super(username, password);
    this.readers = readers;
    this.owners = owners;
  }

  public Set<String> getReaders() {
    return Optional.ofNullable(readers).orElse(Collections.emptySet());
  }

  public Set<String> getOwners() {
    return Optional.ofNullable(owners).orElse(Collections.emptySet());
  }

  public static Credential credentialWithoutPassword(Credential orgCred) {
    return new Credential(orgCred.getUsername(), null, orgCred.getReaders(), orgCred.getOwners());
  }
}
