package org.apache.flink.streaming.api.functions.source.operators;

import org.apache.flink.streaming.api.functions.source.types.SourceSplit;

public class ReaderSplit<SplitT extends SourceSplit> {
  private SplitT split;
  private ReaderSplitState state;

  public ReaderSplit(SplitT split, ReaderSplitState state) {
    this.split = split;
    this.state = state;
  }

  public String splitId() {
    return split.splitId();
  }

  public SplitT getSplit() {
    return split;
  }

  public ReaderSplitState getState() {
    return state;
  }

  public ReaderSplit<SplitT> merge(ReaderSplit<SplitT> other) {
    // use the new state, unless either side is finished then don't allow it to change
    ReaderSplitState newState = other.state;
    if (state == ReaderSplitState.FINISHED || other.state == ReaderSplitState.FINISHED) {
      newState = ReaderSplitState.FINISHED;
    }
    return new ReaderSplit<>(other.split, newState);
  }
}
