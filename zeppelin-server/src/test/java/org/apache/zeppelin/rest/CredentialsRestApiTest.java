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
package org.apache.zeppelin.rest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import java.io.IOException;
import java.lang.reflect.Type;
import java.util.HashSet;
import java.util.Set;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.apache.zeppelin.server.JsonResponse;
import org.apache.zeppelin.service.AuthenticationService;
import org.apache.zeppelin.service.NoAuthenticationService;
import org.apache.zeppelin.user.Credentials;
import org.apache.zeppelin.user.CredentialsMgr;
import org.junit.Before;
import org.junit.Test;

public class CredentialsRestApiTest {
  private final Gson gson = new Gson();

  private CredentialRestApi credentialRestApi;
  private CredentialsMgr credentials;
  private AuthenticationService authenticationService;

  @Before
  public void setUp() throws IOException {
    credentials = new CredentialsMgr();
    authenticationService = new NoAuthenticationService();
    credentialRestApi = new CredentialRestApi(credentials, authenticationService);
  }

  @Test
  public void testInvalidRequest() throws IOException {
    String jsonInvalidRequestEntityNull =
        "{\"entity\" : null, \"username\" : \"test\", " + "\"password\" : \"testpass\"}";
    String jsonInvalidRequestNameNull =
        "{\"entity\" : \"test\", \"username\" : null, " + "\"password\" : \"testpass\"}";
    String jsonInvalidRequestPasswordNull =
        "{\"entity\" : \"test\", \"username\" : \"test\", " + "\"password\" : null}";
    String jsonInvalidRequestAllNull =
        "{\"entity\" : null, \"username\" : null, " + "\"password\" : null}";

    Response response = credentialRestApi.putCredentials(jsonInvalidRequestEntityNull);
    assertEquals(Status.BAD_REQUEST, response.getStatusInfo().toEnum());

    response = credentialRestApi.putCredentials(jsonInvalidRequestNameNull);
    assertEquals(Status.BAD_REQUEST, response.getStatusInfo().toEnum());

    response = credentialRestApi.putCredentials(jsonInvalidRequestPasswordNull);
    assertEquals(Status.BAD_REQUEST, response.getStatusInfo().toEnum());

    response = credentialRestApi.putCredentials(jsonInvalidRequestAllNull);
    assertEquals(Status.BAD_REQUEST, response.getStatusInfo().toEnum());
  }

  public Credentials testGetUserCredentials() {
    Response response = credentialRestApi.getCredentials();
    System.out.println(response.getEntity().toString());
    Type collectionType = new TypeToken<JsonResponse<Credentials>>() {
    }.getType();
    JsonResponse<Credentials> resp = gson.fromJson(response.getEntity().toString(), collectionType);
    return resp.getBody();
  }

  @Test
  public void testCredentialsAPIs() throws IOException {
    String entity = "entityname";

    String requestData1 =
      "{\"entity\":\"entityname\",\"username\":\"myuser\",\"password\":\"mypass\"}";

    Response response = credentialRestApi.putCredentials(requestData1);
    assertEquals(Status.OK, response.getStatusInfo().toEnum());
    assertNotNull("CredentialMap should not be null", testGetUserCredentials());
    assertNotNull("CredentialMap should not be null", testGetUserCredentials().get(entity));
    response = credentialRestApi.removeCredentialEntity("not_exists");
    assertEquals(Status.NOT_FOUND, response.getStatusInfo().toEnum());
    response = credentialRestApi.removeCredentialEntity(entity);
    assertEquals(Status.OK, response.getStatusInfo().toEnum());
    assertNull("CredentialMap should be null", testGetUserCredentials().get(entity));
  }

  @Test
  public void testCredentialsAPIWithRoles() {
    String entity = "entityname";
    AuthenticationService mockAuthenticationService = mock(AuthenticationService.class);
    credentialRestApi = new CredentialRestApi(credentials, mockAuthenticationService);
    Set<String> roles = new HashSet<>();
    roles.add("group1");
    roles.add("group2");
    when(mockAuthenticationService.getPrincipal()).thenReturn("user");
    when(mockAuthenticationService.getAssociatedRoles()).thenReturn(roles);

    // Create credentials
    String requestData1 =
      "{\"entity\":\"entityname\",\"username\":\"myuser\",\"password\":\"mypass\"}";
    Response response = credentialRestApi.putCredentials(requestData1);
    assertEquals(Status.OK, response.getStatusInfo().toEnum());
    assertNotNull("CredentialMap should not be null", testGetUserCredentials().get(entity));
    assertTrue("Reader of CredentialMap should be empty", testGetUserCredentials().get(entity).getReaders().isEmpty());

    // Switch to user2
    when(mockAuthenticationService.getPrincipal()).thenReturn("user2");
    assertTrue("CredentialMap should empty", testGetUserCredentials().isEmpty());
    // Try to delete credentials with user 2
    response = credentialRestApi.removeCredentialEntity(entity);
    assertEquals(Status.FORBIDDEN, response.getStatusInfo().toEnum());

    // Try to update credentials with user2
    String requestData2 =
      "{\"entity\":\"entityname\",\"username\":\"myuser\",\"password\":\"mypass\",\"readers\":[\"group1\"]}";
    response = credentialRestApi.putCredentials(requestData2);
    assertEquals(Status.FORBIDDEN, response.getStatusInfo().toEnum());

    // Switch to user
    when(mockAuthenticationService.getPrincipal()).thenReturn("user");

    // Update credentials
    response = credentialRestApi.putCredentials(requestData2);
    assertEquals(Status.OK, response.getStatusInfo().toEnum());
    assertFalse("CredentialMap should be updated", testGetUserCredentials().get(entity).getReaders().isEmpty());
    assertEquals("Password should be readable the owner", "mypass", testGetUserCredentials().get(entity).getPassword());

    // Switch to user2
    when(mockAuthenticationService.getPrincipal()).thenReturn("user2");
    assertFalse("CredentialMap should now readable", testGetUserCredentials().isEmpty());
    assertFalse("CredentialMap should now readable", testGetUserCredentials().get(entity).getReaders().isEmpty());
    assertNull("Password should be not readable be a reader", testGetUserCredentials().get(entity).getPassword());
    response = credentialRestApi.removeCredentialEntity(entity);
    assertEquals("Deletion should be forbidden, because user2 is only reader", Status.FORBIDDEN, response.getStatusInfo().toEnum());

    // Switch to user
    when(mockAuthenticationService.getPrincipal()).thenReturn("user");
    response = credentialRestApi.removeCredentialEntity(entity);
    assertEquals("Deletion should be allowed, because user is owner", Status.OK, response.getStatusInfo().toEnum());
  }
}
