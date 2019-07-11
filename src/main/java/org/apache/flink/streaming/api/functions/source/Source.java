package org.apache.flink.streaming.api.functions.source;

import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.functions.source.types.Boundedness;
import org.apache.flink.streaming.api.functions.source.types.SourceSplit;

import java.io.IOException;
import java.io.Serializable;

public interface Source<OUT, SplitT extends SourceSplit, EnumChkT> extends Serializable {

  /**
   * Gives a name for this source, should be unique for each instance of the source!
   * @return
   */
  String sourceName();

  /**
   * Checks whether the source supports the given boundedness.
   *
   * <p>Some sources might only support either continuous unbounded streams, or bounded streams.
   */
  boolean supportsBoundedness(Boundedness boundedness);

  /**
   * Creates a new reader to read data from the spits it gets assigned. The reader starts fresh and
   * does not have any state to resume.
   */
  SourceReader<SplitT, OUT> createReader(SourceFunction.SourceContext<OUT> ctx) throws IOException;

  /** Creates a new SplitEnumerator for this source, starting a new input. */
  SplitEnumerator<SplitT, EnumChkT> createEnumerator(Boundedness mode) throws IOException;

  /** Restores an enumerator from a checkpoint. */
  SplitEnumerator<SplitT, EnumChkT> restoreEnumerator(Boundedness mode, EnumChkT checkpoint)
      throws IOException;

  // ------------------------------------------------------------------------
  //  serializers for the metadata
  // ------------------------------------------------------------------------

  /**
   * Creates a serializer for the input splits. Splits are serialized when sending them from
   * enumerator to reader, and when checkpointing the reader's current state.
   */
  SimpleVersionedSerializer<SplitT> getSplitSerializer();

  /**
   * Creates the serializer for the {@link SplitEnumerator} checkpoint. The serializer is used for
   * the result of the {@link SplitEnumerator#snapshotState()} method.
   */
  SimpleVersionedSerializer<EnumChkT> getEnumeratorCheckpointSerializer();
}
