/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.engine.state.message;

import io.zeebe.db.ColumnFamily;
import io.zeebe.db.TransactionContext;
import io.zeebe.db.ZeebeDb;
import io.zeebe.db.impl.DbCompositeKey;
import io.zeebe.db.impl.DbLong;
import io.zeebe.db.impl.DbNil;
import io.zeebe.db.impl.DbString;
import io.zeebe.engine.state.ZbColumnFamilies;
import io.zeebe.engine.state.mutable.MutableWorkflowInstanceSubscriptionState;
import org.agrona.DirectBuffer;

public final class DbWorkflowInstanceSubscriptionState
    implements MutableWorkflowInstanceSubscriptionState {

  private final TransactionContext transactionContext;

  // (elementInstanceKey, messageName) => WorkflowInstanceSubscription
  private final DbLong elementInstanceKey;
  private final DbString messageName;
  private final DbCompositeKey<DbLong, DbString> elementKeyAndMessageName;
  private final WorkflowInstanceSubscription workflowInstanceSubscription;
  private final ColumnFamily<DbCompositeKey<DbLong, DbString>, WorkflowInstanceSubscription>
      subscriptionColumnFamily;

  // (sentTime, elementInstanceKey, messageName) => \0
  private final DbLong sentTime;
  private final DbCompositeKey<DbLong, DbCompositeKey<DbLong, DbString>> sentTimeCompositeKey;
  private final ColumnFamily<DbCompositeKey<DbLong, DbCompositeKey<DbLong, DbString>>, DbNil>
      sentTimeColumnFamily;

  public DbWorkflowInstanceSubscriptionState(
      final ZeebeDb<ZbColumnFamilies> zeebeDb, final TransactionContext transactionContext) {
    this.transactionContext = transactionContext;
    elementInstanceKey = new DbLong();
    messageName = new DbString();
    elementKeyAndMessageName = new DbCompositeKey<>(elementInstanceKey, messageName);
    workflowInstanceSubscription = new WorkflowInstanceSubscription();

    subscriptionColumnFamily =
        zeebeDb.createColumnFamily(
            ZbColumnFamilies.WORKFLOW_SUBSCRIPTION_BY_KEY,
            transactionContext,
            elementKeyAndMessageName,
            workflowInstanceSubscription);

    sentTime = new DbLong();
    sentTimeCompositeKey = new DbCompositeKey<>(sentTime, elementKeyAndMessageName);
    sentTimeColumnFamily =
        zeebeDb.createColumnFamily(
            ZbColumnFamilies.WORKFLOW_SUBSCRIPTION_BY_SENT_TIME,
            transactionContext,
            sentTimeCompositeKey,
            DbNil.INSTANCE);
  }

  @Override
  public void put(final WorkflowInstanceSubscription subscription) {
    wrapSubscriptionKeys(subscription.getElementInstanceKey(), subscription.getMessageName());

    subscriptionColumnFamily.put(elementKeyAndMessageName, subscription);

    sentTime.wrapLong(subscription.getCommandSentTime());
    sentTimeColumnFamily.put(sentTimeCompositeKey, DbNil.INSTANCE);
  }

  @Override
  public WorkflowInstanceSubscription getSubscription(
      final long elementInstanceKey, final DirectBuffer messageName) {
    wrapSubscriptionKeys(elementInstanceKey, messageName);

    return subscriptionColumnFamily.get(elementKeyAndMessageName);
  }

  @Override
  public void visitElementSubscriptions(
      final long elementInstanceKey, final WorkflowInstanceSubscriptionVisitor visitor) {
    this.elementInstanceKey.wrapLong(elementInstanceKey);

    subscriptionColumnFamily.whileEqualPrefix(
        this.elementInstanceKey,
        (compositeKey, subscription) -> {
          visitor.visit(subscription);
        });
  }

  @Override
  public void visitSubscriptionBefore(
      final long deadline, final WorkflowInstanceSubscriptionVisitor visitor) {

    sentTimeColumnFamily.whileTrue(
        (compositeKey, nil) -> {
          final long sentTime = compositeKey.getFirst().getValue();
          if (sentTime < deadline) {
            final WorkflowInstanceSubscription workflowInstanceSubscription =
                subscriptionColumnFamily.get(compositeKey.getSecond());

            return visitor.visit(workflowInstanceSubscription);
          }
          return false;
        });
  }

  @Override
  public void updateToOpenedState(
      final WorkflowInstanceSubscription subscription, final int subscriptionPartitionId) {
    subscription.setOpened();
    subscription.setSubscriptionPartitionId(subscriptionPartitionId);
    updateSentTime(subscription, 0);
  }

  @Override
  public void updateToClosingState(
      final WorkflowInstanceSubscription subscription, final long sentTime) {
    subscription.setClosing();
    updateSentTime(subscription, sentTime);
  }

  @Override
  public void updateSentTimeInTransaction(
      final WorkflowInstanceSubscription subscription, final long sentTime) {
    transactionContext.runInTransaction(() -> updateSentTime(subscription, sentTime));
  }

  @Override
  public void updateSentTime(final WorkflowInstanceSubscription subscription, final long sentTime) {
    wrapSubscriptionKeys(subscription.getElementInstanceKey(), subscription.getMessageName());

    if (subscription.getCommandSentTime() > 0) {
      this.sentTime.wrapLong(subscription.getCommandSentTime());
      sentTimeColumnFamily.delete(sentTimeCompositeKey);
    }

    subscription.setCommandSentTime(sentTime);
    subscriptionColumnFamily.put(elementKeyAndMessageName, subscription);

    if (sentTime > 0) {
      this.sentTime.wrapLong(sentTime);
      sentTimeColumnFamily.put(sentTimeCompositeKey, DbNil.INSTANCE);
    }
  }

  @Override
  public boolean existSubscriptionForElementInstance(
      final long elementInstanceKey, final DirectBuffer messageName) {
    wrapSubscriptionKeys(elementInstanceKey, messageName);

    return subscriptionColumnFamily.exists(elementKeyAndMessageName);
  }

  @Override
  public boolean remove(final long elementInstanceKey, final DirectBuffer messageName) {
    final WorkflowInstanceSubscription subscription =
        getSubscription(elementInstanceKey, messageName);
    final boolean found = subscription != null;
    if (found) {
      remove(subscription);
    }
    return found;
  }

  @Override
  public void remove(final WorkflowInstanceSubscription subscription) {
    wrapSubscriptionKeys(subscription.getElementInstanceKey(), subscription.getMessageName());

    subscriptionColumnFamily.delete(elementKeyAndMessageName);

    sentTime.wrapLong(subscription.getCommandSentTime());
    sentTimeColumnFamily.delete(sentTimeCompositeKey);
  }

  private void wrapSubscriptionKeys(final long elementInstanceKey, final DirectBuffer messageName) {
    this.elementInstanceKey.wrapLong(elementInstanceKey);
    this.messageName.wrapBuffer(messageName);
  }
}
