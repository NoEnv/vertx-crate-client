package com.noenv.crate.codec;

import io.vertx.core.buffer.Buffer;

public class CrateMessage {
  public CrateMessage(Buffer body) {
    System.out.println(body.toString());
  }
}
