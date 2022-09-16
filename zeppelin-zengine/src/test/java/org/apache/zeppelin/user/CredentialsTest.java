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

import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;

import static org.junit.Assert.assertEquals;

public class CredentialsTest {

  @Test
  public void testDefaultProperty() throws IOException {
    CredentialsMgr credentials = new CredentialsMgr();
    Credential up1 = new Credential("user2", "password", null, new HashSet<String>(Arrays.asList("user1")));
    credentials.putCredentialsEntity("hive(vertica)", up1);
    credentials.putCredentialsEntity("user1", up1);
    UsernamePasswords uc2 = credentials.getAllUsernamePasswords(new HashSet<String>(Arrays.asList("user1")));
    UsernamePassword up2 = uc2.getUsernamePassword("hive(vertica)");
    assertEquals(up1.getUsername(), up2.getUsername());
    assertEquals(up1.getPassword(), up2.getPassword());
  }
}
