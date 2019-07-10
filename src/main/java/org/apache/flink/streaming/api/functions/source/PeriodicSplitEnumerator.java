package org.apache.flink.streaming.api.functions.source;

import java.io.IOException;

public interface PeriodicSplitEnumerator<SplitT, CheckpointT>
    extends SplitEnumerator<SplitT, CheckpointT> {

  /**
   * Called periodically to discover further splits.
   *
   * @return Returns true if further splits were discovered, false if not.
   */
  boolean discoverMoreSplits() throws IOException;

  /** Continuous enumeration is only applicable to unbounded sources. */
  default boolean isEndOfInput() {
    return false;
  }
}
