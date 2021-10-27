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

package org.apache.zeppelin.socket;


import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tags;

import org.apache.commons.lang3.StringUtils;
import org.apache.zeppelin.conf.ZeppelinConfiguration;
import org.apache.zeppelin.display.GUI;
import org.apache.zeppelin.display.Input;
import org.apache.zeppelin.notebook.Note;
import org.apache.zeppelin.notebook.NoteInfo;
import org.apache.zeppelin.notebook.NotebookImportDeserializer;
import org.apache.zeppelin.notebook.Paragraph;
import org.apache.zeppelin.notebook.AuthorizationService;
import org.apache.zeppelin.common.Message;
import org.apache.zeppelin.notebook.socket.WatcherMessage;
import org.apache.zeppelin.user.AuthenticationInfo;
import org.apache.zeppelin.util.WatcherSecurityKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.websocket.Session;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Manager class for managing websocket connections
 */
public class ConnectionManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(ConnectionManager.class);
  private static final Gson GSON = new GsonBuilder()
      .setDateFormat("yyyy-MM-dd'T'HH:mm:ssZ")
      .registerTypeAdapter(Date.class, new NotebookImportDeserializer())
      .setPrettyPrinting()
      .registerTypeAdapterFactory(Input.TypeAdapterFactory).create();

  final Queue<Session> connectedSessions = Metrics.gaugeCollectionSize("zeppelin_connected_sessions", Tags.empty(), new ConcurrentLinkedQueue<>());
  // noteId -> connection
  final Map<String, List<Session>> noteSessionMap = Metrics.gaugeMapSize("zeppelin_note_sessions", Tags.empty(), new HashMap<>());
  // user -> connection
  final Map<String, Queue<Session>> userSessionMap = Metrics.gaugeMapSize("zeppelin_user_sessions", Tags.empty(), new HashMap<>());

  /**
   * This is a special endpoint in the notebook websocket, Every connection in this Queue
   * will be able to watch every websocket event, it doesnt need to be listed into the map of
   * noteSocketMap. This can be used to get information about websocket traffic and watch what
   * is going on.
   */
  final Queue<Session> watcherSessions = new ConcurrentLinkedQueue<>();

  private final HashSet<String> collaborativeModeList = Metrics.gaugeCollectionSize("zeppelin_collaborative_modes", Tags.empty(),new HashSet<>());
  private final Boolean collaborativeModeEnable = ZeppelinConfiguration
      .create()
      .isZeppelinNotebookCollaborativeModeEnable();


  private final AuthorizationService authorizationService;

  @Inject
  public ConnectionManager(AuthorizationService authorizationService) {
    this.authorizationService = authorizationService;
  }

  public void addConnection(Session session) {
    connectedSessions.add(session);
  }

  public void removeConnection(Session session) {
    connectedSessions.remove(session);
  }

  public void addNoteConnection(String noteId, Session session) {
    LOGGER.debug("Add Session {} to note: {}", session, noteId);
    synchronized (noteSessionMap) {
      // make sure a socket relates only an single note.
      removeConnectionFromAllNote(session);
      List<Session> socketList = noteSessionMap.computeIfAbsent(noteId, k -> new LinkedList<>());
      if (!socketList.contains(session)) {
        socketList.add(session);
      }
      checkCollaborativeStatus(noteId, socketList);
    }
  }

  public void removeNoteConnection(String noteId) {
    synchronized (noteSessionMap) {
      noteSessionMap.remove(noteId);
    }
  }

  public void removeNoteConnection(String noteId, Session session) {
    LOGGER.debug("Remove connection {} from note: {}", session, noteId);
    synchronized (noteSessionMap) {
      List<Session> socketList = noteSessionMap.getOrDefault(noteId, Collections.emptyList());
      if (!socketList.isEmpty()) {
        socketList.remove(session);
      }
      checkCollaborativeStatus(noteId, socketList);
    }
  }

  public void addUserConnection(Session session) {
    String user = session.getUserPrincipal().getName();
    LOGGER.debug("Add user session {} for user: {}", session, user);
    if (userSessionMap.containsKey(user)) {
      userSessionMap.get(user).add(session);
    } else {
      Queue<Session> socketQueue = new ConcurrentLinkedQueue<>();
      socketQueue.add(session);
      userSessionMap.put(user, socketQueue);
    }
  }

  public void removeUserConnection(Session session) {
    String user = session.getUserPrincipal().getName();
    LOGGER.debug("Remove user connection {} for user: {}", session, session.getUserPrincipal());
    if (userSessionMap.containsKey(user)) {
      userSessionMap.get(user).remove(session);
    } else {
      LOGGER.warn("Closing connection that is absent in user connections");
    }
  }

  public String getAssociatedNoteId(Session session) {
    String associatedNoteId = null;
    synchronized (noteSessionMap) {
      for (Entry<String, List<Session>> noteSocketMapEntry : noteSessionMap.entrySet()) {
        if (noteSocketMapEntry.getValue().contains(session)) {
          associatedNoteId = noteSocketMapEntry.getKey();
        }
      }
    }

    return associatedNoteId;
  }

  public void removeConnectionFromAllNote(Session session) {
    synchronized (noteSessionMap) {
      Set<String> noteIds = noteSessionMap.keySet();
      for (String noteId : noteIds) {
        removeNoteConnection(noteId, session);
      }
    }
  }

  private void checkCollaborativeStatus(String noteId, List<Session> sessionList) {
    if (!collaborativeModeEnable.booleanValue()) {
      return;
    }
    boolean collaborativeStatusNew = sessionList.size() > 1;
    if (collaborativeStatusNew) {
      collaborativeModeList.add(noteId);
    } else {
      collaborativeModeList.remove(noteId);
    }

    Message message = new Message(Message.OP.COLLABORATIVE_MODE_STATUS);
    message.put("status", collaborativeStatusNew);
    if (collaborativeStatusNew) {
      HashSet<String> userList = new HashSet<>();
      for (Session noteSocket : sessionList) {
        userList.add(noteSocket.getUserPrincipal().toString());
      }
      message.put("users", userList);
    }
    broadcast(noteId, message);
  }


  public static String serializeMessage(Message m) {
    return GSON.toJson(m);
  }

  public static Message deserializeMessage(String msg) {
    return GSON.fromJson(msg, Message.class);
  }

  public void broadcast(Message m) {
    synchronized (connectedSessions) {
      for (Session session : connectedSessions) {
        unicast(m, session);
      }
    }
  }

  public void broadcast(String noteId, Message m) {
    List<Session> sessionsToBroadcast;
    synchronized (noteSessionMap) {
      broadcastToWatchers(noteId, StringUtils.EMPTY, m);
      List<Session> socketLists = noteSessionMap.get(noteId);
      if (socketLists == null || socketLists.isEmpty()) {
        return;
      }
      sessionsToBroadcast = new ArrayList<>(socketLists);
    }
    LOGGER.debug("SEND >> {}", m);
    for (Session session : sessionsToBroadcast) {
      unicast(m, session);
    }
  }

  private void broadcastToWatchers(String noteId, String subject, Message message) {
    synchronized (watcherSessions) {
      for (Session watcher : watcherSessions) {
        try {
          watcher.getBasicRemote().sendText(
              WatcherMessage.builder(noteId)
                  .subject(subject)
                  .message(serializeMessage(message))
                  .build()
                  .toJson());
        } catch (IOException | RuntimeException e) {
          LOGGER.error("Cannot broadcast message to watcher", e);
        }
      }
    }
  }

  public void broadcastExcept(String noteId, Message m, Session exclude) {
    List<Session> sessionsToBroadcast;
    synchronized (noteSessionMap) {
      broadcastToWatchers(noteId, StringUtils.EMPTY, m);
      List<Session> socketLists = noteSessionMap.get(noteId);
      if (socketLists == null || socketLists.isEmpty()) {
        return;
      }
      sessionsToBroadcast = new ArrayList<>(socketLists);
    }

    LOGGER.debug("SEND >> {}", m);
    for (Session conn : sessionsToBroadcast) {
      if (exclude.equals(conn)) {
        continue;
      }
      unicast(m, conn);
    }
  }

  public Set<String> getConnectedUsers() {
    Set<String> connectedUsers = new HashSet<>();
    for (Session notebookSessions : connectedSessions) {
      connectedUsers.add(notebookSessions.getUserPrincipal().getName());
    }
    return connectedUsers;
  }


  public void multicastToUser(String user, Message m) {
    if (!userSessionMap.containsKey(user)) {
      LOGGER.warn("Multicasting to user {} that is not in connections map", user);
      return;
    }

    for (Session session : userSessionMap.get(user)) {
      unicast(m, session);
    }
  }

  public void unicast(Message m, Session session) {
    try {
      session.getBasicRemote().sendText(serializeMessage(m));
    } catch (IOException | RuntimeException e) {
      LOGGER.error("socket error", e);
    }
    broadcastToWatchers(StringUtils.EMPTY, StringUtils.EMPTY, m);
  }

  public void unicastParagraph(Note note, Paragraph p, String user, String msgId) {
    if (!note.isPersonalizedMode() || p == null || user == null) {
      return;
    }

    if (!userSessionMap.containsKey(user)) {
      LOGGER.warn("Failed to send unicast. user {} that is not in connections map", user);
      return;
    }

    for (Session session : userSessionMap.get(user)) {
      Message m = new Message(Message.OP.PARAGRAPH).withMsgId(msgId).put("paragraph", p);
      unicast(m, session);
    }
  }

  public interface UserIterator {
    void handleUser(String user, Set<String> userAndRoles);
  }

  public void forAllUsers(UserIterator iterator) {
    for (String user : userSessionMap.keySet()) {
      Set<String> userAndRoles = authorizationService.getRoles(user);
      userAndRoles.add(user);
      iterator.handleUser(user, userAndRoles);
    }
  }

  public void broadcastNoteListExcept(List<NoteInfo> notesInfo,
                                      AuthenticationInfo subject) {
    Set<String> userAndRoles;
    for (String user : userSessionMap.keySet()) {
      if (subject.getUser().equals(user)) {
        continue;
      }
      //reloaded already above; parameter - false
      userAndRoles = authorizationService.getRoles(user);
      userAndRoles.add(user);
      // TODO(zjffdu) is it ok for comment the following line ?
      // notesInfo = generateNotesInfo(false, new AuthenticationInfo(user), userAndRoles);
      multicastToUser(user, new Message(Message.OP.NOTES_INFO).put("notes", notesInfo));
    }
  }

  public void broadcastNote(Note note) {
    broadcast(note.getId(), new Message(Message.OP.NOTE).put("note", note));
  }

  public void broadcastParagraph(Note note, Paragraph p) {
    broadcastNoteForms(note);

    if (note.isPersonalizedMode()) {
      broadcastParagraphs(p.getUserParagraphMap());
    } else {
      broadcast(note.getId(), new Message(Message.OP.PARAGRAPH).put("paragraph", p));
    }
  }

  public void broadcastParagraphs(Map<String, Paragraph> userParagraphMap) {
    if (null != userParagraphMap) {
      for (Entry<String, Paragraph> userParagraphEntry : userParagraphMap.entrySet()) {
        multicastToUser(userParagraphEntry.getKey(),
            new Message(Message.OP.PARAGRAPH).put("paragraph", userParagraphEntry.getValue()));
      }
    }
  }

  private void broadcastNoteForms(Note note) {
    GUI formsSettings = new GUI();
    formsSettings.setForms(note.getNoteForms());
    formsSettings.setParams(note.getNoteParams());
    broadcast(note.getId(), new Message(Message.OP.SAVE_NOTE_FORMS)
        .put("formsData", formsSettings));
  }

  public void switchConnectionToWatcher(Session session) {
    if (!isSessionAllowedToSwitchToWatcher(session)) {
      LOGGER.error("Cannot switch this client to watcher, invalid security key");
      return;
    }
    LOGGER.info("Going to add {} to watcher socket", session);
    // add the connection to the watcher.
    if (watcherSessions.contains(session)) {
      LOGGER.info("connection already present in the watcher");
      return;
    }
    watcherSessions.add(session);

    // remove this connection from regular zeppelin ws usage.
    removeConnection(session);
    removeConnectionFromAllNote(session);
    removeUserConnection(session);
  }

  private boolean isSessionAllowedToSwitchToWatcher(Session session) {

    String watcherSecurityKey = String.valueOf(session.getUserProperties().get(WatcherSecurityKey.HTTP_HEADER));
    return !(StringUtils.isBlank(watcherSecurityKey) || !watcherSecurityKey
        .equals(WatcherSecurityKey.getKey()));
  }
}
