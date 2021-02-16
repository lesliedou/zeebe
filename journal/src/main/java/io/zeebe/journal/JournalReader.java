/*
 * Copyright © 2017 camunda services GmbH (info@camunda.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.zeebe.journal;

import java.util.Iterator;

public interface JournalReader extends Iterator<JournalRecord>, AutoCloseable {

  /**
   * Seek to a record at the given index. if seek(index) return true, {@link JournalReader#next()}
   * should return a record at index.
   *
   * <p>if the index is less than {@link Journal#getFirstIndex()}, {@link JournalReader#next()}
   * should return a record at index {@link Journal#getFirstIndex()}. If the index is greater than
   * {@link Journal#getLastIndex()}, then it seeks to lastIndex + 1, and {@link
   * JournalReader#hasNext()} returns false.
   *
   * @param index the index to seek to.
   * @return true if a record at the index exists, false otherwise.
   */
  long seek(long index);

  /**
   * Seek to the first index of the journal. Equivalent to calling seek(journal.getFirstIndex()).
   */
  long seekToFirst();

  /** Seek to the last index of the journal. Equivalent to calling seek(journal.getLastIndex()). */
  long seekToLast();

  /**
   * Seek to a record with the highest ASQN less than or equal to the given asqn.
   *
   * <p>If there are no records with positive ASQNs, the read will be at the end of the log after
   * this call.
   *
   * <p>If the log is empty, it will return false and the reader will be positioned at the beginning
   * of the log.
   *
   * <p>If the first entry has an ASQN greater than the given asqn, it will return false, and the
   * reader will be positioned at the beginning of the log.
   *
   * <p>If the log is not empty, and no entries have an ASQN less than or equal to the given asqn,
   * but the first entry has ASQN equal to ASQN_IGNORE, it will return true, and the reader will be
   * positioned at the beginning of the log.
   *
   * @param asqn application sequence number to seek
   * @return true if such a record exists, false if there are no records with asqn less than or
   *     equal to the given asqn.
   */
  long seekToAsqn(long asqn, long upperBoundIndex);

  @Override
  void close();
}
