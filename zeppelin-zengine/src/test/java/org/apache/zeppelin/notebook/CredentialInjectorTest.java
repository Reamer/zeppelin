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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.zeppelin.interpreter.InterpreterResult;
import org.apache.zeppelin.interpreter.InterpreterResult.Code;
import org.apache.zeppelin.user.UsernamePassword;
import org.apache.zeppelin.user.UsernamePasswords;
import org.junit.jupiter.api.Test;

class CredentialInjectorTest {

  private static final String TEMPLATE =
    "val jdbcUrl = \"jdbc:mysql://localhost/emp?user={mysql.user}&password={mysql.password}\"";
  private static final String CORRECT_REPLACED =
    "val jdbcUrl = \"jdbc:mysql://localhost/emp?user=username&password=pwd\"";

  private static final String ANSWER =
    "jdbcUrl: String = jdbc:mysql://localhost/employees?user=username&password=pwd";
  private static final String HIDDEN =
    "jdbcUrl: String = jdbc:mysql://localhost/employees?user=username&password=###";

  @Test
  void replaceCredentials() {
    UsernamePasswords userCredentials = mock(UsernamePasswords.class);
    UsernamePassword cred = new UsernamePassword("username", "pwd");
    when(userCredentials.getUsernamePassword("mysql")).thenReturn(cred);
    CredentialInjector testee = new CredentialInjector(userCredentials);
    String actual = testee.replaceCredentials(TEMPLATE);
    assertEquals(CORRECT_REPLACED, actual);

    InterpreterResult ret = new InterpreterResult(Code.SUCCESS, ANSWER);
    InterpreterResult hiddenResult = testee.hidePasswords(ret);
    assertEquals(1, hiddenResult.message().size());
    assertEquals(HIDDEN, hiddenResult.message().get(0).getData());
  }

  @Test
  void replaceCredentialNoTexts() {
    UsernamePasswords userCredentials = mock(UsernamePasswords.class);
    CredentialInjector testee = new CredentialInjector(userCredentials);
    String actual = testee.replaceCredentials(null);
    assertNull(actual);
  }

  @Test
  void replaceCredentialsNotExisting() {
    UsernamePasswords userCredentials = mock(UsernamePasswords.class);
    CredentialInjector testee = new CredentialInjector(userCredentials);
    String actual = testee.replaceCredentials(TEMPLATE);
    assertEquals(TEMPLATE, actual);

    InterpreterResult ret = new InterpreterResult(Code.SUCCESS, ANSWER);
    InterpreterResult hiddenResult = testee.hidePasswords(ret);
    assertEquals(1, hiddenResult.message().size());
    assertEquals(ANSWER, hiddenResult.message().get(0).getData());
  }

  @Test
  void hidePasswordsNoResult() {
    UsernamePasswords userCredentials = mock(UsernamePasswords.class);
    CredentialInjector testee = new CredentialInjector(userCredentials);
    assertNull(testee.hidePasswords(null));
  }

}
