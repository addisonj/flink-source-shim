package org.apache.flink.streaming.api.functions.source;

import org.apache.flink.streaming.api.functions.source.types.SourceSplit;

public interface AsyncSplitReaderFactory<SplitT extends SourceSplit, OUT> {

  AsyncSplitReader<OUT> create(SplitT split);
}
