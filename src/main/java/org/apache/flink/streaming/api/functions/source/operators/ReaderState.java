package org.apache.flink.streaming.api.functions.source.operators;

import org.apache.flink.streaming.api.functions.source.types.ReaderLocation;
import org.apache.flink.streaming.api.functions.source.types.SourceSplit;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ReaderState<SplitT extends SourceSplit> {
  private String name;
  private ReaderLocation location;
  private SourceStateCommand.ReaderCommandState state;
  private Map<String, ReaderSplit<SplitT>> splits;

  public ReaderState(String name, ReaderLocation loc) {
    this.name = name;
    this.location = loc;
    this.state = SourceStateCommand.ReaderCommandState.NEW_READER;
    this.splits = new HashMap<>();
  }

  public ReaderLocation getLocation() {
    return location;
  }

  public Map<String, ReaderSplit<SplitT>> getSplitStates() {
    return splits;
  }

  public List<ReaderSplit<SplitT>> getUnfinishedSplits() {
    return getSplitStates().values().stream()
        .filter(
            (split) ->
                split.getState() == ReaderSplitState.WAITING
                    || split.getState() == ReaderSplitState.PROCESSING)
        .collect(Collectors.toList());
  }

  public boolean waitingForSplits() {
    return this.state == SourceStateCommand.ReaderCommandState.SEND_MORE;
  }

  public ReaderState<SplitT> updateState(
      SourceStateCommand.ReaderCommandState newState, List<ReaderSplit<SplitT>> updates) {
    ReaderState<SplitT> updatedState = new ReaderState<>(name, location);
    updatedState.state = newState;
    if (newState == SourceStateCommand.ReaderCommandState.FINISHED
        || state == SourceStateCommand.ReaderCommandState.FINISHED) {
      updatedState.state = SourceStateCommand.ReaderCommandState.FINISHED;
    }
    // we merge states by
    Map<String, ReaderSplit<SplitT>> newSplitStates = new HashMap<>();
    splits.forEach(newSplitStates::put);
    updates.forEach((state) -> newSplitStates.merge(state.splitId(), state, ReaderSplit::merge));
    updatedState.splits = newSplitStates;
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
    // we merge states by
    Map<String, ReaderSplit<SplitT>> newSplitStates = new HashMap<>();
    splits.forEach(newSplitStates::put);
    other
        .getSplitStates()
        .forEach((k, v) -> newSplitStates.merge(k, v, ReaderSplit<SplitT>::merge));
    mergedState.splits = newSplitStates;
    return mergedState;
  }
}
