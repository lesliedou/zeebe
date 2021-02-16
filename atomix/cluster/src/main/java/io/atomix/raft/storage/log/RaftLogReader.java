/*
 * Copyright 2017-present Open Networking Foundation
 * Copyright © 2020 camunda services GmbH (info@camunda.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.atomix.raft.storage.log;

import io.atomix.raft.storage.log.entry.RaftLogEntry;
import io.atomix.raft.zeebe.ZeebeEntry;
import io.atomix.storage.journal.Indexed;
import io.zeebe.journal.JournalReader;
import io.zeebe.journal.JournalRecord;
import java.nio.ByteBuffer;
import java.util.NoSuchElementException;
import org.agrona.DirectBuffer;

/** Raft log reader. */
public class RaftLogReader implements java.util.Iterator<Indexed<RaftLogEntry>>, AutoCloseable {
  private final RaftLog log;
  private final JournalReader journalReader;
  private final RaftLogReader.Mode mode;

  // NOTE: nextIndex is only used if the reader is in commit mode, hence why it's not subject to
  // inconsistencies when the log is truncated/compacted/etc.
  private long nextIndex;

  RaftLogReader(
      final RaftLog log, final JournalReader journalReader, final RaftLogReader.Mode mode) {
    this.log = log;
    this.journalReader = journalReader;
    this.mode = mode;
  }

  @Override
  public boolean hasNext() {
    final boolean isCommitBound = mode == Mode.COMMITS && nextIndex <= log.getCommitIndex();
    return journalReader.hasNext() && !isCommitBound;
  }

  @Override
  public Indexed<RaftLogEntry> next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }

    final JournalRecord journalRecord = journalReader.next();
    final RaftLogEntry entry = deserialize(journalRecord.data());

    nextIndex = journalRecord.index() + 1;
    return new Indexed<>(
        journalRecord.index(), entry, journalRecord.data().capacity(), journalRecord.checksum());
  }

  public long reset() {
    nextIndex = journalReader.seekToFirst();
    return nextIndex;
  }

  public long reset(final long index) {
    final long boundedIndex = mode == Mode.COMMITS ? Math.min(index, log.getCommitIndex()) : index;
    nextIndex = journalReader.seek(boundedIndex);
    return nextIndex;
  }

  public long seekToLast() {
    if (mode == Mode.ALL) {
      nextIndex = journalReader.seekToLast();
    } else {
      reset(log.getCommitIndex());
    }

    return nextIndex;
  }

  public long seekToAsqn(final long asqn) {
    nextIndex = journalReader.seekToAsqn(asqn);

    // should happen very seldom, as most of the time we seek to committed ASQNs; the solution is
    // therefore not very efficient. should be optimized if this becomes more common.
    // see https://github.com/zeebe-io/zeebe/issues/6343
    if (mode == Mode.COMMITS) {
      final long commitIndex = log.getCommitIndex();
      if (nextIndex > commitIndex) {
        seekToLastApplicationEntry(commitIndex);
      }
    }

    return nextIndex;
  }

  @Override
  public void close() {
    journalReader.close();
  }

  /**
   * Deserializes given DirectBuffer to Object using Kryo instance in pool.
   *
   * @param buffer input with serialized bytes
   * @param <T> deserialized Object type
   * @return deserialized Object
   */
  private <T> T deserialize(final DirectBuffer buffer) {
    final ByteBuffer byteBufferView;

    if (buffer.byteArray() != null) {
      byteBufferView =
          ByteBuffer.wrap(buffer.byteArray(), buffer.wrapAdjustment(), buffer.capacity());
    } else {
      byteBufferView =
          buffer
              .byteBuffer()
              .asReadOnlyBuffer()
              .position(buffer.wrapAdjustment())
              .limit(buffer.wrapAdjustment() + buffer.capacity());
    }

    return log.getSerializer().deserialize(byteBufferView);
  }

  private void seekToLastApplicationEntry(final long upperBoundIndex) {
    reset(upperBoundIndex);

    while (hasNext()) {
      final Indexed<RaftLogEntry> entry = next();
      if (isApplicationEntry(entry)) {
        // reset so we can read it next
        reset(entry.index());
        return;
      } else {
        // only way to iterate back is to seek to the previous index
        reset(entry.index() - 1);

        // if we will read the same index next, then it means we've reached the beginning of the log
        // so there is no application entry
        if (nextIndex == entry.index()) {
          reset();
          return;
        }
      }
    }
  }

  private boolean isApplicationEntry(final Indexed<RaftLogEntry> entry) {
    return entry.type() == ZeebeEntry.class;
  }

  /** Raft log reader mode. */
  public enum Mode {

    /** Reads all entries from the log. */
    ALL,

    /** Reads committed entries from the log. */
    COMMITS,
  }
}
