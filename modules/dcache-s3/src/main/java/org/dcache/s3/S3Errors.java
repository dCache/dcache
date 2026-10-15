/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2026 Deutsches Elektronen-Synchrotron
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.dcache.s3;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.FileNotFoundCacheException;
import diskCacheV111.util.FileNotOnlineCacheException;
import diskCacheV111.util.NotDirCacheException;
import diskCacheV111.util.PermissionDeniedCacheException;
import diskCacheV111.util.TimeoutCacheException;
import org.gaul.s3proxy.S3ErrorCode;
import org.gaul.s3proxy.S3ProxyException;
import org.gaul.s3proxy.blobstore.S3Exceptions;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.http.SdkHttpResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;

/**
 * The S3 errors the dCache blob store answers with, and their mapping from dCache failures.
 * <p>
 * Errors that s3proxy's {@link S3ErrorCode} lacks are built as SDK exceptions carrying their own
 * error code, which the door renders like any other.
 */
final class S3Errors {

    private static final Logger LOGGER = LoggerFactory.getLogger(S3Errors.class);

    private S3Errors() {
    }

    /**
     * The 503 that asks the client to retry later.
     */
    static S3Exception serviceUnavailable(String message, @Nullable Throwable cause) {
        return error(503, "ServiceUnavailable", message, cause);
    }

    /**
     * The 403 that S3 answers a read of an archived object with.
     */
    static S3Exception invalidObjectState() {
        return error(403, "InvalidObjectState",
              "The operation is not valid for the object's storage class", null);
    }

    static S3Exception accessDenied(@Nullable Throwable cause) {
        return new S3ProxyException(S3ErrorCode.ACCESS_DENIED, cause);
    }

    static S3Exception internalError(@Nullable Throwable cause) {
        return new S3ProxyException(S3ErrorCode.INTERNAL_ERROR, cause);
    }

    /**
     * Maps a failed namespace or transfer operation to an S3 error.
     *
     * @param bucket the bucket of the request
     * @param key    the object key, or null if the request is about the bucket itself
     */
    static S3Exception from(CacheException e, String bucket, @Nullable String key) {
        if (e instanceof FileNotFoundCacheException || e instanceof NotDirCacheException) {
            return key == null
                  ? S3Exceptions.noSuchBucket(bucket, e.getMessage(), e)
                  : S3Exceptions.noSuchKey(bucket, key, e.getMessage(), e);
        }
        if (e instanceof PermissionDeniedCacheException) {
            return accessDenied(e);
        }
        if (e instanceof FileNotOnlineCacheException) {
            return invalidObjectState();
        }
        if (e instanceof TimeoutCacheException) {
            return serviceUnavailable(e.getMessage(), e);
        }
        switch (e.getRc()) {
            case CacheException.SERVICE_UNAVAILABLE:
            case CacheException.NO_POOL_ONLINE:
            case CacheException.POOL_UNAVAILABLE:
            case CacheException.POOL_DISABLED:
            case CacheException.RESOURCE:
                return serviceUnavailable(e.getMessage(), e);
            default:
                LOGGER.warn("S3 request on {}/{} failed: {}", bucket, key, e.toString());
                return internalError(e);
        }
    }

    /**
     * Maps an interrupted operation: the door is shutting down, so the client should retry.
     */
    static S3Exception interrupted(InterruptedException e) {
        Thread.currentThread().interrupt();
        return serviceUnavailable("The request was interrupted", e);
    }

    private static S3Exception error(int status, String code, String message,
          @Nullable Throwable cause) {
        return (S3Exception) S3Exception.builder()
              .message(message)
              .cause(cause)
              .statusCode(status)
              .awsErrorDetails(AwsErrorDetails.builder()
                    .errorCode(code)
                    .errorMessage(message)
                    .sdkHttpResponse(SdkHttpResponse.builder().statusCode(status).build())
                    .build())
              .build();
    }
}
