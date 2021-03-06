/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.snapshots.broker.impl;

import io.zeebe.snapshots.broker.SnapshotId;
import io.zeebe.snapshots.raft.PersistedSnapshot;
import io.zeebe.snapshots.raft.TransientSnapshot;
import io.zeebe.util.FileUtil;
import io.zeebe.util.sched.ActorControl;
import io.zeebe.util.sched.future.ActorFuture;
import io.zeebe.util.sched.future.CompletableActorFuture;
import java.io.IOException;
import java.nio.file.Path;
import java.util.function.BiConsumer;
import java.util.function.Predicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Represents a pending snapshot, that is a snapshot in the process of being written and has not yet
 * been committed to the store.
 */
public final class FileBasedTransientSnapshot implements TransientSnapshot {
  private static final Logger LOGGER = LoggerFactory.getLogger(FileBasedTransientSnapshot.class);

  private final Path directory;
  private final ActorControl actor;
  private final FileBasedSnapshotStore snapshotStore;
  private final FileBasedSnapshotMetadata metadata;
  private final ActorFuture<Boolean> takenFuture = new CompletableActorFuture<>();
  private boolean isValid = false;

  FileBasedTransientSnapshot(
      final FileBasedSnapshotMetadata metadata,
      final Path directory,
      final FileBasedSnapshotStore snapshotStore,
      final ActorControl actor) {
    this.metadata = metadata;
    this.snapshotStore = snapshotStore;
    this.directory = directory;
    this.actor = actor;
  }

  @Override
  public ActorFuture<Boolean> take(final Predicate<Path> takeSnapshot) {
    actor.run(() -> takeInternal(takeSnapshot));
    return takenFuture;
  }

  @Override
  public void onSnapshotTaken(final BiConsumer<Boolean, Throwable> runnable) {
    actor.call(() -> takenFuture.onComplete(runnable));
  }

  private void takeInternal(final Predicate<Path> takeSnapshot) {
    final var snapshotMetrics = snapshotStore.getSnapshotMetrics();

    try (final var ignored = snapshotMetrics.startTimer()) {
      try {
        isValid = takeSnapshot.test(getPath());
        if (!isValid) {
          abortInternal();
        } else if (!directory.toFile().exists() || directory.toFile().listFiles().length == 0) {
          // If no snapshot files are created, snapshot is not valid
          isValid = false;
        } else {
          calculateAndPersistChecksum();
        }
        takenFuture.complete(isValid);
      } catch (final Exception exception) {
        LOGGER.warn("Unexpected exception on taking snapshot ({})", metadata, exception);
        abortInternal();
        takenFuture.completeExceptionally(exception);
      }
    }
  }

  private void calculateAndPersistChecksum() throws IOException {
    final var checksum = SnapshotChecksum.calculate(directory);
    SnapshotChecksum.persist(directory, checksum);
  }

  @Override
  public ActorFuture<Void> abort() {
    final CompletableActorFuture<Void> abortFuture = new CompletableActorFuture<>();
    actor.run(
        () -> {
          abortInternal();
          abortFuture.complete(null);
        });
    return abortFuture;
  }

  @Override
  public ActorFuture<PersistedSnapshot> persist() {
    final CompletableActorFuture<PersistedSnapshot> future = new CompletableActorFuture<>();
    actor.call(
        () -> {
          if (!takenFuture.isDone()) {
            future.completeExceptionally(new IllegalStateException("Snapshot is not taken"));
            return;
          }
          if (!isValid) {
            future.completeExceptionally(
                new IllegalStateException("Snapshot is not valid. It may have been deleted."));
            return;
          }
          if (!SnapshotChecksum.hasChecksum(getPath())) {
            future.completeExceptionally(
                new IllegalStateException("Snapshot is not valid. There is no checksum file."));
            return;
          }
          try {
            final var snapshot = snapshotStore.newSnapshot(metadata, directory);
            future.complete(snapshot);
          } catch (final Exception e) {
            future.completeExceptionally(e);
          }
        });
    return future;
  }

  @Override
  public SnapshotId snapshotId() {
    return metadata;
  }

  private void abortInternal() {
    try {
      isValid = false;
      LOGGER.debug("DELETE dir {}", directory);
      FileUtil.deleteFolder(directory);
    } catch (final IOException e) {
      LOGGER.warn("Failed to delete pending snapshot {}", this, e);
    } finally {
      snapshotStore.removePendingSnapshot(this);
    }
  }

  private Path getPath() {
    return directory;
  }

  @Override
  public String toString() {
    return "FileBasedTransientSnapshot{"
        + "directory="
        + directory
        + ", snapshotStore="
        + snapshotStore
        + ", metadata="
        + metadata
        + '}';
  }
}
