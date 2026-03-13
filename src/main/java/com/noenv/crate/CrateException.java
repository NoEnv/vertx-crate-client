package com.noenv.crate;

/**
 * Exception thrown when a CrateDB request fails.
 * <p>
 * Carries the HTTP status, CrateDB error code, message, and optionally an error stack trace
 * when requested via {@code error_trace=true}.
 * </p>
 *
 * @see #getHttpStatus()
 * @see #getErrorCode()
 * @see #getErrorTrace()
 */
public class CrateException extends RuntimeException {

  private final int httpStatus;
  private final int errorCode;
  private final String errorTrace;

  /**
   * Creates an exception with the given HTTP status, error code, and message.
   *
   * @param httpStatus the HTTP response status code
   * @param errorCode  the CrateDB error code
   * @param message    the error message
   */
  public CrateException(int httpStatus, int errorCode, String message) {
    this(httpStatus, errorCode, message, null);
  }

  /**
   * Creates an exception with the given HTTP status, error code, message, and optional error trace.
   *
   * @param httpStatus the HTTP response status code
   * @param errorCode  the CrateDB error code
   * @param message    the error message
   * @param errorTrace the error stack trace from CrateDB, or null
   */
  public CrateException(int httpStatus, int errorCode, String message, String errorTrace) {
    super(message);
    this.httpStatus = httpStatus;
    this.errorCode = errorCode;
    this.errorTrace = errorTrace;
  }

  /**
   * Returns the HTTP response status code from the failed request.
   *
   * @return the HTTP status code (e.g. 400, 404, 500)
   */
  public int getHttpStatus() {
    return httpStatus;
  }

  /**
   * Returns the CrateDB-specific error code.
   *
   * @return the error code
   */
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
