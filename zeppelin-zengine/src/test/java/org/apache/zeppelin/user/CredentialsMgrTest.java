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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;

import org.apache.commons.io.IOUtils;
import org.junit.Before;
import org.junit.Test;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class CredentialsMgrTest {

  private Gson gson;

  @Before
  public void startUp() throws IOException {
    GsonBuilder builder = new GsonBuilder();
    builder.setPrettyPrinting();
    gson = builder.create();
  }

  @Test
  public void testCredentialLoad() throws IOException {
    Credentials credentials = new Credentials();
    HashSet<String> reader = new HashSet<>();
    reader.add("reader1");
    HashSet<String> owner = new HashSet<>();
    owner.add("user1");
    owner.add("user2");
    credentials.put("enitity1", new Credential("username", "password", reader, owner));
    credentials.put("enitity2", new Credential("username2", "password2", reader, owner));

    String output = gson.toJson(new CredentialsInfoSaving(credentials));
    assertNotNull(output);
    Credentials creds = CredentialsMgr.parseToCredentials(output, gson);
    assertEquals("username", creds.get("enitity1").getUsername());
    assertEquals("password", creds.get("enitity1").getPassword());
    assertEquals("username2", creds.get("enitity2").getUsername());
    assertEquals("password2", creds.get("enitity2").getPassword());
    // All readers and owners are part of the data structure.
    assertEquals(2, creds.get("enitity1").getOwners().size());
    assertTrue(creds.get("enitity1").getOwners().containsAll(owner));
    assertEquals(1, creds.get("enitity1").getReaders().size());
    assertTrue(creds.get("enitity1").getReaders().containsAll(reader));

    // Generate old output
    output = gson.toJson(new CredentialsInfoSavingOld(credentials));
    assertNotNull(output);
    creds = CredentialsMgr.parseToCredentials(output, gson);
    assertEquals("username", creds.get("enitity1").getUsername());
    assertEquals("password", creds.get("enitity1").getPassword());
    assertEquals("username2", creds.get("enitity2").getUsername());
    assertEquals("password2", creds.get("enitity2").getPassword());
    // readers and owners cannot be transferred to the old data structure.
    assertEquals(1, creds.get("enitity1").getOwners().size());
    assertEquals(0, creds.get("enitity1").getReaders().size());
  }

  @Test
  public void testLoading() throws IOException {
    String credentials = IOUtils.toString(CredentialsMgrTest.class.getResourceAsStream("/credentials/credentials.json"), StandardCharsets.UTF_8);
    assertNotNull(credentials);
    Credentials creds = CredentialsMgr.parseToCredentials(credentials, gson);
    assertEquals(2, creds.get("enitity1").getOwners().size());
    assertEquals(1, creds.get("enitity1").getReaders().size());
    assertEquals("user1", creds.get("enitity1").getOwners().iterator().next());
    assertEquals("username", creds.get("enitity1").getUsername());
    assertEquals("password", creds.get("enitity1").getPassword());

    assertEquals(1, creds.get("enitity2").getOwners().size());
    assertEquals(2, creds.get("enitity2").getReaders().size());
    assertEquals("user1", creds.get("enitity2").getOwners().iterator().next());
    assertEquals("username2", creds.get("enitity2").getUsername());
    assertEquals("password2", creds.get("enitity2").getPassword());

  }

  @Test
  public void testMigration() throws IOException {
    String credentials_old = IOUtils.toString(CredentialsMgrTest.class.getResourceAsStream("/credentials/credentials_old.json"), StandardCharsets.UTF_8);
    assertNotNull(credentials_old);
    Credentials creds = CredentialsMgr.parseToCredentials(credentials_old, gson);
    assertNotNull(creds);

    assertEquals(1, creds.get("test").getOwners().size());
    assertEquals("user1", creds.get("test").getOwners().iterator().next());
    assertEquals("test", creds.get("test").getUsername());
    assertEquals("test", creds.get("test").getPassword());

    assertEquals(1, creds.get("test2").getOwners().size());
    assertEquals("user1", creds.get("test2").getOwners().iterator().next());
    assertEquals("username", creds.get("test2").getUsername());
    assertEquals("password", creds.get("test2").getPassword());

    assertEquals(1, creds.get("FOO").getOwners().size());
    assertEquals("user2", creds.get("FOO").getOwners().iterator().next());
    assertEquals("2", creds.get("FOO").getUsername());
    assertEquals("1", creds.get("FOO").getPassword());
  }
}
