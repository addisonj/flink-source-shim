package org.apache.flink.streaming.api.functions.source.operators;

import org.apache.flink.streaming.api.functions.source.types.SourceSplit;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class SplitQuery<SplitT extends SourceSplit> {
  private Map<String, ReaderState<SplitT>> readerStates;

  public SplitQuery() {
    this.readerStates = new HashMap<>();
  }

  private SplitQuery(Map<String, ReaderState<SplitT>> states) {
    this.readerStates = states;
  }

  private Map<String, ReaderState<SplitT>> getReaderStates() {
    return readerStates;
  }

  public SplitQuery<SplitT> updateReader(String name, ReaderState<SplitT> state) {
    readerStates.put(name, state);
    return this;
  }

  public Optional<ReaderState<SplitT>> getReaderState(String name) {
    return Optional.ofNullable(readerStates.get(name));
  }

  public List<ReaderState<SplitT>> getWaitingReaders() {
    return readerStates.values().stream()
        .filter(ReaderState::waitingForSplits)
        .collect(Collectors.toList());
  }

  public SplitQuery<SplitT> merge(SplitQuery<SplitT> other) {
    Map<String, ReaderState<SplitT>> newState = new HashMap<>();
    readerStates.forEach(newState::put);
    other.getReaderStates().forEach((k, v) -> newState.merge(k, v, ReaderState<SplitT>::merge));
    return new SplitQuery<>(newState);
  }
}
