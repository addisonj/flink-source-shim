package org.apache.flink.streaming.api.functions.source.operators;

import org.apache.flink.streaming.api.functions.source.types.ReaderLocation;
import org.apache.flink.streaming.api.functions.source.types.SourceSplit;

public class ReadRequest<SplitT extends SourceSplit> {
  private final SplitT split;
  private final ReaderLocation location;
  private boolean enumeratorFinished;

  public ReadRequest(SplitT split, ReaderLocation loc) {
    this.split = split;
    this.location = loc;
    this.enumeratorFinished = false;
  }

  public SplitT getSplit() {
    return split;
  }

  public ReaderLocation getLocation() {
    return location;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof ReadRequest) {
      ReadRequest<?> rr = (ReadRequest) obj;

      return split.equals(rr.split)
          && location.equals(rr.location)
          && enumeratorFinished == rr.enumeratorFinished;
    } else {
      return false;
    }
  }

  @SuppressWarnings("unchecked")
  private static ReadRequest buildFinished() {
    ReadRequest<?> rr = new ReadRequest(null, new ReaderLocation("finished"));
    rr.enumeratorFinished = true;
    return rr;
  }

  static ReadRequest LAST_SPLIT = buildFinished();
}
