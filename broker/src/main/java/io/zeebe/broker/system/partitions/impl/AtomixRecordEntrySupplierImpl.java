/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.broker.system.partitions.impl;

import io.atomix.raft.storage.log.RaftLogReader;
import io.atomix.raft.storage.log.entry.RaftLogEntry;
import io.atomix.storage.journal.Indexed;
import io.zeebe.broker.system.partitions.AtomixRecordEntrySupplier;
import java.util.Optional;

public final class AtomixRecordEntrySupplierImpl implements AtomixRecordEntrySupplier {
  private final RaftLogReader reader;

  public AtomixRecordEntrySupplierImpl(final RaftLogReader reader) {
    this.reader = reader;
  }

  @Override
  public Optional<Indexed<RaftLogEntry>> getIndexedEntry(final long position) {
    // the index returned either means we are at the position, or at the nearest lower position, or
    // at the very beginning of the log. in practice, this should always return the position unless
    // there was a major bug and it doesn't exist, so always return the next entry for now.
    reader.seekToAsqn(position);

    if (reader.hasNext()) {
      return Optional.of(reader.next());
    }

    return Optional.empty();
  }

  @Override
  public void close() {
    reader.close();
  }
}
