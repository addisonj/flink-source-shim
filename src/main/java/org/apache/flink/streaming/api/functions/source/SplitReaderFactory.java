package org.apache.flink.streaming.api.functions.source;

import org.apache.flink.streaming.api.functions.source.types.SourceSplit;

public interface SplitReaderFactory<SplitT extends SourceSplit, OUT> {

  SplitReader<OUT> create(SplitT split);
}
