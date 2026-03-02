package com.noenv.crate;

public class CrateException extends RuntimeException {

  private final int httpStatus;
  private final int errorCode;

  public CrateException(int httpStatus, int errorCode, String message) {
    super(message);
    this.httpStatus = httpStatus;
    this.errorCode = errorCode;
  }

  public int getHttpStatus() {
    return httpStatus;
  }

  public int getErrorCode() {
    return errorCode;
  }

  @Override
  public String toString() {
    return "CrateException{httpStatus=" + httpStatus + ", errorCode=" + errorCode + ", message=" + getMessage() + "}";
  }
}
