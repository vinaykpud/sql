/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.scan.context;

import org.apache.calcite.rel.RelNode;

/**
 * Represents a push down operation that can be applied to an OpenSearchRequestBuilder.
 *
 * @param type PushDownType enum
 * @param digest the digest of the pushed down operator
 * @param action the lambda action to apply on the OpenSearchRequestBuilder
 * @param relNode the RelNode for this push-down operation (used for Substrait conversion)
 */
public record PushDownOperation(
    PushDownType type, Object digest, AbstractAction<?> action, RelNode relNode) {
  
  /**
   * Constructor for backward compatibility (without RelNode)
   */
  public PushDownOperation(PushDownType type, Object digest, AbstractAction<?> action) {
    this(type, digest, action, null);
  }
  
  public String toString() {
    return type + "->" + digest;
  }
}
