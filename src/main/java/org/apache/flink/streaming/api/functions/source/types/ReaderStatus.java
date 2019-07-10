package org.apache.flink.streaming.api.functions.source.types;

public enum ReaderStatus {
  MORE_AVAILABLE,
  NOTHING_AVAILABLE,
  END_OF_SPLIT_DATA
}
