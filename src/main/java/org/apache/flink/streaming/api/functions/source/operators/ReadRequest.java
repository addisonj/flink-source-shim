package org.apache.flink.streaming.api.functions.source.operators;

import org.apache.flink.streaming.api.functions.source.types.ReaderLocation;
import org.apache.flink.streaming.api.functions.source.types.SourceSplit;

public class ReadRequest<SplitT extends SourceSplit> {
  private final SplitT split;
  private final ReaderLocation location;

  public ReadRequest(SplitT split, ReaderLocation loc) {
    this.split = split;
    this.location = loc;
  }

  public SplitT getSplit() {
    return split;
  }

  public ReaderLocation getLocation() {
    return location;
  }
}
