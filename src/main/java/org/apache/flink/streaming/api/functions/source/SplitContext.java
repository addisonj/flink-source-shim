package org.apache.flink.streaming.api.functions.source;

import java.io.IOException;

public interface SplitContext {
  void requestNewSplit() throws IOException;
}
