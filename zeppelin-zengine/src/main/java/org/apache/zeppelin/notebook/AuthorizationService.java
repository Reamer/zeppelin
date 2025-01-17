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

import org.apache.commons.lang3.StringUtils;
import org.apache.zeppelin.conf.ZeppelinConfiguration;
import org.apache.zeppelin.storage.ConfigStorage;
import org.apache.zeppelin.user.AuthenticationInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.io.IOException;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This class is responsible for maintain notes authorization info. And provide api for
 * setting and querying note authorization info.
 */
public class AuthorizationService {

  private static final Logger LOGGER = LoggerFactory.getLogger(AuthorizationService.class);
  private static final Set<String> EMPTY_SET = new HashSet<>();

  private final ZeppelinConfiguration zConf;
  private final ConfigStorage configStorage;

  private static final Set<ZeppelinConfiguration.ConfVars> VALID_ROLES_CONF_VARS = EnumSet.of(
      ZeppelinConfiguration.ConfVars.ZEPPELIN_OWNER_ROLES,
      ZeppelinConfiguration.ConfVars.ZEPPELIN_WRITER_ROLES,
      ZeppelinConfiguration.ConfVars.ZEPPELIN_READER_ROLES,
      ZeppelinConfiguration.ConfVars.ZEPPELIN_RUNNER_ROLES);

  // contains roles for each user (username --> roles)
  private Map<String, Set<String>> userRoles = new ConcurrentHashMap<>();

  // cached note permission info. (noteId --> NoteAuth)
  private Map<String, NoteAuth> notesAuth = new ConcurrentHashMap<>();

  @Inject
  public AuthorizationService(NoteManager noteManager, ZeppelinConfiguration zConf,
      ConfigStorage storage) {
    LOGGER.info("Injected AuthorizationService: {}", this);
    this.zConf = zConf;
    this.configStorage = storage;
    try {
      // init notesAuth by reading notebook-authorization.json
      NotebookAuthorizationInfoSaving authorizationInfoSaving = configStorage.loadNotebookAuthorization();
      if (authorizationInfoSaving != null) {
        for (Map.Entry<String, Map<String, Set<String>>> entry : authorizationInfoSaving.getAuthInfo().entrySet()) {
          String noteId = entry.getKey();
          Map<String, Set<String>> permissions = entry.getValue();
          notesAuth.put(noteId, new NoteAuth(noteId, permissions, zConf));
        }
      }

      // initialize NoteAuth for the notes without permission set explicitly.
      for (String noteId : noteManager.getNotesInfo().keySet()) {
        if (!notesAuth.containsKey(noteId)) {
          notesAuth.put(noteId, new NoteAuth(noteId, zConf));
        }
      }
    } catch (IOException e) {
      throw new RuntimeException("Fail to create ConfigStorage", e);
    }
  }

  /**
   * Create NoteAuth, this method only create NoteAuth in memory, you need to call method
   * saveNoteAuth to persistent it to storage.
   * @param noteId
   * @param subject
   * @throws IOException
   */
  public void createNoteAuth(String noteId, AuthenticationInfo subject) {
    NoteAuth noteAuth = new NoteAuth(noteId, subject, zConf);
    this.notesAuth.put(noteId, noteAuth);
  }

  /**
   * Persistent NoteAuth
   *
   * @throws IOException
   */
  public synchronized void saveNoteAuth() throws IOException {
    configStorage.save(new NotebookAuthorizationInfoSaving(this.notesAuth));
  }

  public void removeNoteAuth(String noteId) {
    this.notesAuth.remove(noteId);
  }

  // skip empty user and remove the white space around user name.
  private Set<String> normalizeUsers(Set<String> users) {
    Set<String> returnUser = new HashSet<>();
    for (String user : users) {
      String trimmedUser = user.trim();
      if (!trimmedUser.isEmpty()) {
        returnUser.add(trimmedUser);
      }
    }
    return returnUser;
  }

  public void setOwners(String noteId, Set<String> entities) throws IOException {
    setOwners(noteId, entities, true);
  }

  public void setReaders(String noteId, Set<String> entities) throws IOException {
    setReaders(noteId, entities, true);
  }

  public void setWriters(String noteId, Set<String> entities) throws IOException {
    setWriters(noteId, entities, true);
  }

  public void setRunners(String noteId, Set<String> entities) throws IOException {
    setRunners(noteId, entities, true);
  }

  public void setRoles(String user, Set<String> roles) {
    setRoles(user, roles, true);
  }

  public void clearPermission(String noteId) throws IOException {
    clearPermission(noteId, true);
  }

  public void setOwners(String noteId, Set<String> entities, boolean broadcast) throws IOException {
    entities = normalizeUsers(entities);
    NoteAuth noteAuth = notesAuth.get(noteId);
    if (noteAuth == null) {
      throw new IOException("No noteAuth found for noteId: " + noteId);
    }
    noteAuth.setOwners(entities);
  }

  public void setReaders(String noteId, Set<String> entities, boolean broadcast) throws IOException {
    entities = normalizeUsers(entities);
    NoteAuth noteAuth = notesAuth.get(noteId);
    if (noteAuth == null) {
      throw new IOException("No noteAuth found for noteId: " + noteId);
    }
    noteAuth.setReaders(entities);
  }

  public void setRunners(String noteId, Set<String> entities, boolean broadcast) throws IOException {
    entities = normalizeUsers(entities);
    NoteAuth noteAuth = notesAuth.get(noteId);
    if (noteAuth == null) {
      throw new IOException("No noteAuth found for noteId: " + noteId);
    }
    noteAuth.setRunners(entities);
  }

  public void setWriters(String noteId, Set<String> entities, boolean broadcast) throws IOException {
    entities = normalizeUsers(entities);
    NoteAuth noteAuth = notesAuth.get(noteId);
    if (noteAuth == null) {
      throw new IOException("No noteAuth found for noteId: " + noteId);
    }
    noteAuth.setWriters(entities);
  }

  public void setRoles(String user, Set<String> roles, boolean broadcast) {
    if (StringUtils.isBlank(user)) {
      LOGGER.warn("Setting roles for empty user");
      return;
    }
    roles = normalizeUsers(roles);
    userRoles.put(user, roles);
  }

  public void clearPermission(String noteId, boolean broadcast) throws IOException {
    NoteAuth noteAuth = notesAuth.get(noteId);
    if (noteAuth == null) {
      throw new IOException("No noteAuth found for noteId: " + noteId);
    }
    noteAuth.setReaders(new HashSet<>());
    noteAuth.setRunners(new HashSet<>());
    noteAuth.setWriters(new HashSet<>());
    noteAuth.setOwners(new HashSet<>());

  }

  public Set<String> getOwners(String noteId) {
    NoteAuth noteAuth = notesAuth.get(noteId);
    if (noteAuth == null) {
      LOGGER.warn("No noteAuth found for noteId: {}", noteId);
      return EMPTY_SET;
    }
    return noteAuth.getOwners();
  }

  public Set<String> getReaders(String noteId) {
    NoteAuth noteAuth = notesAuth.get(noteId);
    if (noteAuth == null) {
      LOGGER.warn("No noteAuth found for noteId: {}", noteId);
      return EMPTY_SET;
    }
    return noteAuth.getReaders();
  }

  public Set<String> getRunners(String noteId) {
    NoteAuth noteAuth = notesAuth.get(noteId);
    if (noteAuth == null) {
      LOGGER.warn("No noteAuth found for noteId: {}", noteId);
      return EMPTY_SET;
    }
    return noteAuth.getRunners();
  }

  public Set<String> getWriters(String noteId) {
    NoteAuth noteAuth = notesAuth.get(noteId);
    if (noteAuth == null) {
      LOGGER.warn("No noteAuth found for noteId: {}", noteId);
      return EMPTY_SET;
    }
    return noteAuth.getWriters();
  }

  public Set<String> getRoles(String user) {
    return userRoles.getOrDefault(user, new HashSet<>());
  }

  public boolean isOwner(String noteId, Set<String> entities) {
    return isMember(entities, constructRoles(getOwners(noteId), getDefaultOwners())) ||
           isAdmin(entities, zConf);
  }

  public boolean isWriter(String noteId, Set<String> entities) {
    return isMember(entities, constructRoles(getWriters(noteId), getDefaultWriters())) ||
           isMember(entities, constructRoles(getOwners(noteId), getDefaultOwners())) ||
           isAdmin(entities, zConf);
  }

  public boolean isReader(String noteId, Set<String> entities) {
    return isMember(entities, constructRoles(getReaders(noteId), getDefaultReaders())) ||
           isMember(entities, constructRoles(getOwners(noteId), getDefaultOwners())) ||
           isMember(entities, constructRoles(getWriters(noteId), getDefaultWriters())) ||
           isMember(entities, constructRoles(getRunners(noteId), getDefaultRunners())) ||
           isAdmin(entities, zConf);
  }

  public boolean isRunner(String noteId, Set<String> entities) {
    return isMember(entities, constructRoles(getRunners(noteId), getDefaultRunners())) ||
           isMember(entities, constructRoles(getWriters(noteId), getDefaultWriters())) ||
           isMember(entities, constructRoles(getOwners(noteId), getDefaultOwners())) ||
           isAdmin(entities, zConf);
  }

  private Set<String> constructRoles(Set<String> noteRoles, Set<String> globalRoles) {
    Set<String> roles = new HashSet<>(noteRoles);
    // If the note has no role, the note right is for everyone, so we are not allowed to add the default roles
    if (!roles.isEmpty()) {
      roles.addAll(globalRoles);
    }
    return roles;
  }

  private Set<String> getDefaultOwners() {
    return getDefaultRoles(ZeppelinConfiguration.ConfVars.ZEPPELIN_OWNER_ROLES);
  }

  private Set<String> getDefaultWriters() {
    return getDefaultRoles(ZeppelinConfiguration.ConfVars.ZEPPELIN_WRITER_ROLES);
  }

  private Set<String> getDefaultReaders() {
    return getDefaultRoles(ZeppelinConfiguration.ConfVars.ZEPPELIN_READER_ROLES);
  }

  private Set<String> getDefaultRunners() {
    return getDefaultRoles(ZeppelinConfiguration.ConfVars.ZEPPELIN_RUNNER_ROLES);
  }

  private Set<String> getDefaultRoles(ZeppelinConfiguration.ConfVars confvar) {
    if (!VALID_ROLES_CONF_VARS.contains(confvar)) {
      LOGGER.warn("getDefaultRoles is used with {}, which is not valid", confvar);
      return Collections.emptySet();
    }
    Set<String> defaultRoles = new HashSet<>();
    String defaultRolesConf = zConf.getString(confvar);
    if (StringUtils.isNotBlank(defaultRolesConf)) {
      Collections.addAll(defaultRoles, StringUtils.split(defaultRolesConf, ','));
    }
    return normalizeUsers(defaultRoles);
  }

  /**
   * @param entities - Username and roles of the current user
   * @param zconf - ZeppelinConfiguration, where we can get the owner role
   * @return true if the user or role is part of the owner role
   */
  public static boolean isAdmin(Set<String> entities, ZeppelinConfiguration zConf) {
    String adminRole = zConf.getString(ZeppelinConfiguration.ConfVars.ZEPPELIN_OWNER_ROLE);
    if (StringUtils.isBlank(adminRole)) {
      return false;
    }
    return entities.contains(adminRole);
  }

  /**
   * Checks whether the user with his roles is a member of a group. Attention: If the group is empty, the user is a member of
   * the group.
   *
   * @param a - Username and roles of the current user
   * @param b - Configured users and roles on the stored object
   * @return true if b is empty or if (b intersection a) is non-empty
   */
  public static boolean isMember(Set<String> a, Set<String> b) {
    Set<String> intersection = new HashSet<>(b);
    intersection.retainAll(a);
    return (b.isEmpty() || !intersection.isEmpty());
  }

  public boolean isOwner(Set<String> userAndRoles, String noteId) {
    if (zConf.isAnonymousAllowed()) {
      LOGGER.debug("Zeppelin runs in anonymous mode, everybody is owner");
      return true;
    }
    if (userAndRoles == null) {
      return false;
    }
    return isOwner(noteId, userAndRoles);
  }

  //TODO(zjffdu) merge this hasWritePermission with isWriter ?
  public boolean hasWritePermission(Set<String> userAndRoles, String noteId) {
    if (zConf.isAnonymousAllowed()) {
      LOGGER.debug("Zeppelin runs in anonymous mode, everybody is writer");
      return true;
    }
    if (userAndRoles == null) {
      return false;
    }
    return isWriter(noteId, userAndRoles);
  }

  public boolean hasReadPermission(Set<String> userAndRoles, String noteId) {
    if (zConf.isAnonymousAllowed()) {
      LOGGER.debug("Zeppelin runs in anonymous mode, everybody is reader");
      return true;
    }
    if (userAndRoles == null) {
      return false;
    }
    return isReader(noteId, userAndRoles);
  }

  public boolean hasRunPermission(Set<String> userAndRoles, String noteId) {
    if (zConf.isAnonymousAllowed()) {
      LOGGER.debug("Zeppelin runs in anonymous mode, everybody is reader");
      return true;
    }
    if (userAndRoles == null) {
      return false;
    }
    return isRunner(noteId, userAndRoles);
  }

  public boolean isPublic() {
    return zConf.isNotebookPublic();
  }

}
