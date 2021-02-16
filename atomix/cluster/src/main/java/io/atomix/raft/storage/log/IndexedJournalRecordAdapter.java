package io.atomix.raft.storage.log;

import io.atomix.raft.storage.log.entry.RaftLogEntry;
import io.atomix.storage.journal.Indexed;
import io.zeebe.journal.JournalRecord;
import org.agrona.DirectBuffer;

public final class IndexedJournalRecordAdapter implements JournalRecord {
  private final Indexed<RaftLogEntry> indexedEntry;
  private final DirectBuffer data;

  public IndexedJournalRecordAdapter(
      final Indexed<RaftLogEntry> indexedEntry, final DirectBuffer data) {
    this.indexedEntry = indexedEntry;
    this.data = data;
  }

  @Override
  public long index() {
    return indexedEntry.index();
  }

  @Override
  public long asqn() {
    return -1;
  }

  @Override
  public int checksum() {
    return (int) indexedEntry.checksum();
  }

  @Override
  public DirectBuffer data() {
    return data;
  }
}
