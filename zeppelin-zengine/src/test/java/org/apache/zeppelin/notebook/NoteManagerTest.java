package org.apache.zeppelin.notebook;

import org.apache.zeppelin.conf.ZeppelinConfiguration;
import org.apache.zeppelin.notebook.exception.NotePathAlreadyExistsException;
import org.apache.zeppelin.notebook.repo.InMemoryNotebookRepo;
import org.apache.zeppelin.user.AuthenticationInfo;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class NoteManagerTest {
  private NoteManager noteManager;
  private ZeppelinConfiguration conf;

  @Rule
  public ExpectedException thrown = ExpectedException.none();


  @Before
  public void setUp() throws IOException {
    conf = ZeppelinConfiguration.create();
    this.noteManager = new NoteManager(new InMemoryNotebookRepo(), conf);
  }

  @Test
  public void testNoteOperations() throws IOException {
    assertEquals(0, this.noteManager.getNotesInfo().size());

    Note note1 = createNote("/prod/my_note1");
    Note note2 = createNote("/dev/project_2/my_note2");
    Note note3 = createNote("/dev/project_3/my_note3");

    // Fake loaded state
    note1.setLoaded(true);
    note2.setLoaded(true);
    note3.setLoaded(true);

    // add note
    this.noteManager.saveNote(note1);
    this.noteManager.saveNote(note2);
    this.noteManager.saveNote(note3);

    // list notes
    assertEquals(3, this.noteManager.getNotesInfo().size());
    assertEquals(note1, this.noteManager.getNote(note1.getId()));
    assertEquals(note2, this.noteManager.getNote(note2.getId()));
    assertEquals(note3, this.noteManager.getNote(note3.getId()));

    // move note
    this.noteManager.moveNote(note1.getId(), "/dev/project_1/my_note1",
        AuthenticationInfo.ANONYMOUS);
    assertEquals(3, this.noteManager.getNotesInfo().size());
    assertEquals("/dev/project_1/my_note1", this.noteManager.getNote(note1.getId()).getPath());

    // move folder
    this.noteManager.moveFolder("/dev", "/staging", AuthenticationInfo.ANONYMOUS);
    Map<String, String> notesInfo = this.noteManager.getNotesInfo();
    assertEquals(3, notesInfo.size());
    assertEquals("/staging/project_1/my_note1", notesInfo.get(note1.getId()));
    assertEquals("/staging/project_2/my_note2", notesInfo.get(note2.getId()));
    assertEquals("/staging/project_3/my_note3", notesInfo.get(note3.getId()));

    this.noteManager.removeNote(note1.getId(), AuthenticationInfo.ANONYMOUS);
    assertEquals(2, this.noteManager.getNotesInfo().size());

    // remove folder
    this.noteManager.removeFolder("/staging", AuthenticationInfo.ANONYMOUS);
    notesInfo = this.noteManager.getNotesInfo();
    assertEquals(0, notesInfo.size());
  }

  @Test
  public void testAddNoteRejectsDuplicatePath() throws IOException {
    thrown.expect(NotePathAlreadyExistsException.class);
    thrown.expectMessage("Note '/prod/note' existed");

    Note note1 = createNote("/prod/note");
    Note note2 = createNote("/prod/note");

    noteManager.addNote(note1, AuthenticationInfo.ANONYMOUS);
    noteManager.addNote(note2, AuthenticationInfo.ANONYMOUS);
  }

  @Test
  public void testMoveNoteRejectsDuplicatePath() throws IOException {
    thrown.expect(NotePathAlreadyExistsException.class);
    thrown.expectMessage("Note '/prod/note-1' existed");

    Note note1 = createNote("/prod/note-1");
    Note note2 = createNote("/prod/note-2");

    noteManager.addNote(note1, AuthenticationInfo.ANONYMOUS);
    noteManager.addNote(note2, AuthenticationInfo.ANONYMOUS);

    noteManager.moveNote(note2.getId(), "/prod/note-1", AuthenticationInfo.ANONYMOUS);
  }

  private Note createNote(String notePath) {
    return new Note(notePath, "test", null, null, null, null, null);
  }

  @Test
  public void testLruCache() throws IOException {

    int cacheThreshold = conf.getNoteCacheThreshold();

    // fill cache
    for (int i = 0; i < cacheThreshold; ++i) {
      Note note = createNote("/prod/note" + i);
      noteManager.addNote(note, AuthenticationInfo.ANONYMOUS);
    }
    assertEquals(cacheThreshold, noteManager.getCacheSize());

    // add cache + 1
    Note noteNew = createNote("/prod/notenew");
    noteManager.addNote(noteNew, AuthenticationInfo.ANONYMOUS);
    // check for first eviction
    assertEquals(cacheThreshold, noteManager.getCacheSize());

    // add notes with read flag
    for (int i = 0; i < cacheThreshold; ++i) {
      Note note = createNote("/prod/noteDirty" + i);
      note.getReadLock().lock();
      noteManager.addNote(note, AuthenticationInfo.ANONYMOUS);
    }
    assertEquals(cacheThreshold, noteManager.getCacheSize());

    // add cache + 1
    Note noteNew2 = createNote("/prod/notenew2");
    noteManager.addNote(noteNew2, AuthenticationInfo.ANONYMOUS);

    // since all notes in the cache are dirty, the cache grows
    assertEquals(cacheThreshold + 1, noteManager.getCacheSize());


  }
}
