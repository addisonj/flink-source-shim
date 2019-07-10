package org.apache.flink.streaming.api.functions.source.operators;

public enum ReaderSplitState {
  WAITING, // the split is not yet being processed
  PROCESSING, // the split is being processed
  FINISHED // the split is finished
}
