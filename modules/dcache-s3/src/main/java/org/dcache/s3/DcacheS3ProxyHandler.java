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

import com.google.common.base.Strings;
import java.io.IOException;
import java.util.Map;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.gaul.s3proxy.BlobStoreLocator;
import org.gaul.s3proxy.S3ErrorCode;
import org.gaul.s3proxy.S3ProxyException;
import org.gaul.s3proxy.S3ProxyHandler;
import org.gaul.s3proxy.auth.AuthenticationType;

/**
 * s3proxy's S3 protocol handler, configured for the dCache door: every request is signed and
 * served by the blob store {@code locator} finds for its access key.
 * <p>
 * The handler is given no default blob store, so unsigned requests are refused with
 * {@code AccessDenied} rather than served anonymously.
 */
public class DcacheS3ProxyHandler extends S3ProxyHandler {

    /**
     * The largest request body s3proxy buffers to verify a signature that covers the payload,
     * as in upstream s3proxy.
     */
    private static final long MAX_NON_CHUNKED_REQUEST_SIZE = 128L * 1024 * 1024;

    /**
     * The largest chunk of an aws-chunked upload, as in upstream s3proxy.
     */
    private static final int MAX_CHUNK_SIZE = 16 * 1024 * 1024;

    /**
     * The largest object a single PUT may upload, as in S3.
     */
    private static final long MAX_SINGLE_PART_OBJECT_SIZE = 5L * 1024 * 1024 * 1024;

    /**
     * @param locator            finds the secret and blob store of an access key
     * @param authenticationType the signature versions accepted
     * @param virtualHost        the host name under which buckets are addressed as
     *                           {@code bucket.host}; empty to address buckets by path only
     * @param maximumTimeSkew    how far, in seconds, a request's date may be from the door's
     */
    public DcacheS3ProxyHandler(BlobStoreLocator locator, AuthenticationType authenticationType,
          String virtualHost, int maximumTimeSkew) {
        // The identity and credential are required by the constructor, but the locator that
        // uses them is replaced below.
        super(null, authenticationType, "", "", Strings.emptyToNull(virtualHost),
              MAX_SINGLE_PART_OBJECT_SIZE, MAX_NON_CHUNKED_REQUEST_SIZE, MAX_CHUNK_SIZE,
              false, null, null, maximumTimeSkew);
        if (authenticationType == AuthenticationType.NONE) {
            throw new IllegalArgumentException("S3 requests must be signed");
        }
        setBlobStoreLocator(locator);
    }

    /**
     * Sends an S3 error document.
     */
    public void sendError(HttpServletRequest request, HttpServletResponse response, String code,
          int status, String message) throws IOException {
        sendSimpleErrorResponse(request, response, code, status, message, Map.of());
    }

    /**
     * Sends an S3 error document with the code's own message.
     */
    public void sendError(HttpServletRequest request, HttpServletResponse response,
          S3ErrorCode code) throws IOException {
        String message = new S3ProxyException(code).awsErrorDetails().errorMessage();
        sendSimpleErrorResponse(request, response, code, message, Map.of());
    }
}
