/*
 * Copyright 2014-2026 Andrew Gaul <andrew@gaul.org>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.gaul.s3proxy.auth;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;

import org.junit.Test;

public final class S3AuthorizationHeaderTest {

    @Test
    public void parsesAwsV2Header() {
        var header = new S3AuthorizationHeader("AWS access-key:abc123/sig=");
        assertEquals(AuthenticationType.AWS_V2, header.getAuthenticationType());
        assertEquals("access-key", header.getIdentity());
        assertEquals("abc123/sig=", header.getSignature());
        assertNull(header.getHmacAlgorithm());
        assertNull(header.getHashAlgorithm());
        assertNull(header.getRegion());
        assertNull(header.getDate());
        assertNull(header.getService());
    }

    @Test
    public void parsesAwsV4Sha256Header() {
        var raw = "AWS4-HMAC-SHA256 " +
                "Credential=AKIAIOSFODNN7EXAMPLE/20260101/us-east-1/s3/" +
                "aws4_request, SignedHeaders=host;x-amz-date, " +
                "Signature=abc123";
        var header = new S3AuthorizationHeader(raw);
        assertEquals(AuthenticationType.AWS_V4, header.getAuthenticationType());
        assertEquals("AKIAIOSFODNN7EXAMPLE", header.getIdentity());
        assertEquals("20260101", header.getDate());
        assertEquals("us-east-1", header.getRegion());
        assertEquals("s3", header.getService());
        assertEquals("HmacSHA256", header.getHmacAlgorithm());
        assertEquals("SHA-256", header.getHashAlgorithm());
        assertEquals("abc123", header.getSignature());
    }

    @Test
    public void parsesAwsV4Sha1Header() {
        var raw = "AWS4-HMAC-SHA1 " +
                "Credential=key/20260101/us-west-2/s3/aws4_request, " +
                "SignedHeaders=host, Signature=deadbeef";
        var header = new S3AuthorizationHeader(raw);
        assertEquals("HmacSHA1", header.getHmacAlgorithm());
        assertEquals("SHA-1", header.getHashAlgorithm());
        assertEquals("us-west-2", header.getRegion());
    }

    @Test
    public void signatureCanBeTheLastField() {
        // No trailing comma after Signature= -- extractSignature must handle
        // both "Signature=…," and "Signature=…" at end-of-string.
        var raw = "AWS4-HMAC-SHA256 " +
                "Credential=k/20260101/us-east-1/s3/aws4_request, " +
                "SignedHeaders=host, Signature=zzz";
        var header = new S3AuthorizationHeader(raw);
        assertEquals("zzz", header.getSignature());
    }

    @Test
    public void rejectsUnknownAuthScheme() {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> new S3AuthorizationHeader("Bearer token"));
        assertEquals("Invalid header", e.getMessage());
    }

    @Test
    public void rejectsAwsV2WithoutColon() {
        assertThrows(IllegalArgumentException.class,
                () -> new S3AuthorizationHeader("AWS access-key-no-colon"));
    }

    @Test
    public void rejectsAwsV2WithExtraSpaces() {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> new S3AuthorizationHeader("AWS too many parts here"));
        assertEquals("Invalid header", e.getMessage());
    }

    @Test
    public void rejectsAwsV4WithoutSpace() {
        assertThrows(IllegalArgumentException.class,
                () -> new S3AuthorizationHeader("AWS4-HMAC-SHA256"));
    }

    @Test
    public void rejectsAwsV4UnsupportedAlgorithm() {
        var raw = "AWS4-HMAC-SHA512 " +
                "Credential=k/20260101/us-east-1/s3/aws4_request, " +
                "SignedHeaders=host, Signature=zzz";
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> new S3AuthorizationHeader(raw));
        assertThat(e.getMessage(), containsString("Unsupported signing algorithm"));
    }

    @Test
    public void rejectsAwsV4WithoutCredentialField() {
        var raw = "AWS4-HMAC-SHA256 SignedHeaders=host, Signature=zzz";
        assertThrows(IllegalArgumentException.class, () -> new S3AuthorizationHeader(raw));
    }

    @Test
    public void rejectsAwsV4MalformedCredential() {
        // Only 3 slash-separated parts in Credential= rather than 5.
        var raw = "AWS4-HMAC-SHA256 Credential=k/20260101/s3, " +
                "SignedHeaders=host, Signature=zzz";
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> new S3AuthorizationHeader(raw));
        assertThat(e.getMessage(), containsString("Invalid Credential"));
    }

    @Test
    public void rejectsAwsV4WithoutSignatureField() {
        var raw = "AWS4-HMAC-SHA256 " +
                "Credential=k/20260101/us-east-1/s3/aws4_request, " +
                "SignedHeaders=host";
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> new S3AuthorizationHeader(raw));
        assertThat(e.getMessage(), containsString("Invalid signature"));
    }
}
