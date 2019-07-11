package org.apache.flink.streaming.api.functions.source.operators;

import org.apache.flink.streaming.api.functions.source.types.ReaderLocation;
import org.apache.flink.streaming.api.functions.source.types.SourceSplit;

public class ReaderState<SplitT extends SourceSplit> {
  private String name;
  private ReaderLocation location;
  private SourceStateCommand.ReaderCommandState state;

  public ReaderState(String name, ReaderLocation loc) {
    this.name = name;
    this.location = loc;
    this.state = SourceStateCommand.ReaderCommandState.NEW_READER;
  }

  public ReaderLocation getLocation() {
    return location;
  }

  public boolean waitingForSplits() {
    return this.state == SourceStateCommand.ReaderCommandState.SEND_MORE;
  }

  public ReaderState<SplitT> updateState(SourceStateCommand.ReaderCommandState newState) {
    ReaderState<SplitT> updatedState = new ReaderState<>(name, location);
    updatedState.state = newState;
    if (newState == SourceStateCommand.ReaderCommandState.FINISHED
        || state == SourceStateCommand.ReaderCommandState.FINISHED) {
      updatedState.state = SourceStateCommand.ReaderCommandState.FINISHED;
    }
    return updatedState;
  }

  public ReaderState<SplitT> merge(ReaderState<SplitT> other) {
    ReaderState<SplitT> mergedState = new ReaderState<>(name, location);
    mergedState.state = other.state;
    // if either side is finished, new state is always finished (we can't moved out of finished)
    if (other.state == SourceStateCommand.ReaderCommandState.FINISHED
        || state == SourceStateCommand.ReaderCommandState.FINISHED) {
      mergedState.state = SourceStateCommand.ReaderCommandState.FINISHED;
    }
    return mergedState;
  }
}
