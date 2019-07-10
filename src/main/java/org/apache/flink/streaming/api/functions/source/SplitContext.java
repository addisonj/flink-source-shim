package org.apache.flink.streaming.api.functions.source;

public interface SplitContext {
  void requestNewSplit();
}
