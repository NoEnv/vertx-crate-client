package com.noenv.crate;

public class CrateException extends RuntimeException {

  private final int httpStatus;
  private final int errorCode;
  private final String errorTrace;

  public CrateException(int httpStatus, int errorCode, String message) {
    this(httpStatus, errorCode, message, null);
  }

  public CrateException(int httpStatus, int errorCode, String message, String errorTrace) {
    super(message);
    this.httpStatus = httpStatus;
    this.errorCode = errorCode;
    this.errorTrace = errorTrace;
  }

  public int getHttpStatus() {
    return httpStatus;
  }

  public int getErrorCode() {
    return errorCode;
  }

  /**
   * Error stack trace from CrateDB when requested via {@code error_trace=true}.
   *
   * @return the error_trace string, or null if not requested or not present
   */
  public String getErrorTrace() {
    return errorTrace;
  }

  @Override
  public String toString() {
    return "CrateException{httpStatus=" + httpStatus + ", errorCode=" + errorCode + ", message=" + getMessage() + "}";
  }
}
