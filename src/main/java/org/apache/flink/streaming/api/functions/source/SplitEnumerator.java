package org.apache.flink.streaming.api.functions.source;

import org.apache.flink.streaming.api.functions.source.types.ReaderLocation;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Optional;

public interface SplitEnumerator<SplitT, CheckpointT> extends Closeable {

  /**
   * Returns true when the input is bounded and no more splits are available. False means that the
   * definite end of input has been reached, and is only possible in bounded sources.
   *
   * <p>TODO: the above comment seems incorrect
   */
  boolean isEndOfInput();

  /**
   * Returns the next split, if it is available. If nothing is currently available, this returns an
   * empty Optional. More may be available later, if the {@link #isEndOfInput()} is false.
   */
  Optional<SplitT> nextSplit(ReaderLocation reader);

  /**
   * Adds splits back to the enumerator. This happens when a reader failed and restarted, and the
   * splits assigned to that reader since the last checkpoint need to be made available again.
   *
   * <p>TODO: this isn't currently called by this implementation see details in SourceOperator `initializeState`
   */
  void addSplitsBack(List<SplitT> splits);

  /** Checkpoints the state of this split enumerator. */
  CheckpointT snapshotState();

  /**
   * Called to close the enumerator, in case it holds on to any resources, like threads or network
   * connections.
   */
  @Override
  void close() throws IOException;
}
