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


import java.io.IOException;
import java.util.Map.Entry;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.zeppelin.conf.ZeppelinConfiguration;
import org.apache.zeppelin.storage.ConfigStorage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

/**
 * Class defining credentials for data source authorization
 */
public class CredentialsMgr {

  private static final Logger LOGGER = LoggerFactory.getLogger(CredentialsMgr.class);

  private ConfigStorage storage;
  private Credentials credentials;
  private final Gson gson;
  private Encryptor encryptor;

  /**
   * Wrapper for user credentials. It can load credentials from a file
   * and will encrypt the file if an encryptKey is configured.
   *
   * @param conf
   * @throws IOException
   */
  public CredentialsMgr(ZeppelinConfiguration conf) {
    credentials = new Credentials();
    if (conf.credentialsPersist()) {
      String encryptKey = conf.getCredentialsEncryptKey();
      if (StringUtils.isNotBlank(encryptKey)) {
        this.encryptor = new Encryptor(encryptKey);
      }
      GsonBuilder builder = new GsonBuilder();
      builder.setPrettyPrinting();
      gson = builder.create();
      try {
        storage = ConfigStorage.getInstance(conf);
        loadFromFile();
      } catch (IOException e) {
        LOGGER.error("Fail to create ConfigStorage for Credentials. Persistenz will be disabled", e);
        encryptor = null;
        storage = null;
      }
    } else {
      encryptor = null;
      storage = null;
      gson = null;
    }
  }

  /**
   * Wrapper for inmemory user credentials.
   *
   * @param conf
   * @throws IOException
   */
  public CredentialsMgr() {
    credentials = new Credentials();
    encryptor = null;
    storage = null;
    gson = null;
  }

  public UsernamePasswords getAllUsernamePasswords(Set<String> userAndRoles) {
    UsernamePasswords up = new UsernamePasswords();
    up.putAll(getAllReadableCredentials(userAndRoles));
    return up;
  }

  public Credential getCredentialByEntity(String entity) {
    return credentials.get(entity);
  }

  public Credentials getAllReadableCredentials(Set<String> userAndRoles) {
    Credentials sharedCreds = new Credentials();
    for (Entry<String, Credential> cred : credentials.entrySet()) {
      if (isReader(cred.getValue(), userAndRoles)) {
        sharedCreds.putCredential(cred.getKey(), cred.getValue());
      }
    }
    return sharedCreds;
  }

  public static boolean isReader(Credential cred, Set<String> userAndRoles) {
    return isMember(cred.getReaders(), userAndRoles) || isOwner(cred, userAndRoles);
  }

  public static boolean isOwner(Credential cred, Set<String> userAndRoles) {
    return isMember(cred.getOwners(), userAndRoles);
  }

  /**
   * Intersection of a and b
   *
   * @param Set of roles
   * @param Set of roles
   * @return true if (a intersection b) is non-empty
   */
  private static boolean isMember(Set<String> a, Set<String> b) {
    Set<String> intersection = new HashSet<>(b);
    intersection.retainAll(a);
    return !intersection.isEmpty();
  }

  public boolean exists(String enitiy) {
    return credentials.containsEntity(enitiy);
  }

  public boolean isOwner(String entity, Set<String> userAndRoles) {
    Credential cred = credentials.get(entity);
    return isMember(cred.getOwners(), userAndRoles);
  }

  public void putCredentialsEntity(String entity, Credential cred) throws IOException {
    loadCredentials();
    credentials.putCredential(entity, cred);
    saveCredentials();
  }

  /**
   *
   * @param entity to remove
   * @return true if the credentials entity was removed, false if the credentials entity was not found and not deleted
   * @throws IOException
   */
  public boolean removeCredentialEntity(String entity) throws IOException {
    loadCredentials();
    if (!credentials.containsEntity(entity)) {
      return false;
    }

    credentials.removeCredential(entity);
    saveCredentials();
    return true;
  }

  public void saveCredentials() throws IOException {
    if (storage != null) {
      saveToFile();
    }
  }

  private void loadCredentials() throws IOException {
    if (storage != null) {
      loadFromFile();
    }
  }

  private void loadFromFile() throws IOException {
    try {
      String json = storage.loadCredentials();
      if (json != null && encryptor != null) {
        json = encryptor.decrypt(json);
      }

      Credentials loadedCreds = parseToCredentials(json, gson);
      if (!loadedCreds.isEmpty()) {
        credentials = loadedCreds;
      }
    } catch (IOException e) {
      throw new IOException("Error loading credentials file", e);
    }
  }

  public static Credentials parseToCredentials(String json, Gson gson) {
    CredentialsInfoSaving info = gson.fromJson(json, CredentialsInfoSaving.class);
    if (info.getCredentialsMap() != null) {
      return info.getCredentialsMap();
    }
    LOGGER.warn("Parsing with CredentialsInfoSavingOld");
    CredentialsInfoSavingOld infoOld = gson.fromJson(json, CredentialsInfoSavingOld.class);
    return infoOld.getCredentialsMap();
  }

  private void saveToFile() throws IOException {
    String jsonString;

    synchronized (credentials) {
      CredentialsInfoSaving info = new CredentialsInfoSaving(credentials);
      jsonString = gson.toJson(info);
    }

    try {
      if (encryptor != null) {
        jsonString = encryptor.encrypt(jsonString);
      }
      storage.saveCredentials(jsonString);
    } catch (IOException e) {
      throw new IOException("Error saving credentials file", e);
    }
  }
}
