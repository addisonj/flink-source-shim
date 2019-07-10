package org.apache.flink.streaming.api.functions.source.types;

import java.util.UUID;

public class ReaderLocation {
  private final String location;
  public ReaderLocation(String location) {
    this.location = location;
  }

  static public ReaderLocation randomLocation() {
    return new ReaderLocation(UUID.randomUUID().toString());
  }
}
