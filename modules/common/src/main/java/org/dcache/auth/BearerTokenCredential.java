package org.dcache.auth;

import static com.google.common.base.Preconditions.checkArgument;
import static java.nio.charset.StandardCharsets.US_ASCII;

import com.google.common.base.CharMatcher;
import com.google.common.hash.Hashing;
import java.io.Serializable;
import java.util.Base64;

public class BearerTokenCredential implements Serializable {

    private static final long serialVersionUID = -5933313664563503235L;
    private final String _token;

    public BearerTokenCredential(String token) {
        checkArgument(CharMatcher.ascii().matchesAllOf(token), "Bearer Token not ASCII");
        _token = token;
    }

    /**
     * Provide the token.  Important: this method must not be used if the returned value may be
     * logged.
     *
     * @return The BearerToken with which the user is authenticating.
     */
    public String getToken() {
        return _token;
    }

    private String hash(String in) {
        byte[] hashBytes = new byte[8];
        Hashing.sha256().hashBytes(in.getBytes(US_ASCII))
              .writeBytesTo(hashBytes, 0, hashBytes.length);
        return "Hash=" + Base64.getEncoder().withoutPadding().encodeToString(hashBytes);
    }

    /**
     * Provide a reasonable description of the token without revealing the complete token.  The
     * output is intended for human consumption (e.g., admin commands, log files) and tries to
     * balance providing enough information to check whether two tokens are (very likely) the same
     * without leaking the complete token.
     *
     * @return a summary of the token that is safe to log
     */
    public String describeToken() {
        int length = _token.length();

        if (length <= 8) {
            return hash(_token);
        } else {
            String head = _token.substring(0, 4);
            String middle = _token.substring(4, length - 4);
            String tail = _token.substring(length - 4, length);

            return head + "+{" + hash(middle) + "}+" + tail;
        }
    }

    @Override
    public int hashCode() {
        return _token.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }

        if (!(o instanceof BearerTokenCredential)) {
            return false;
        }

        BearerTokenCredential other = (BearerTokenCredential) o;

        return _token.equals(other._token);
    }

    @Override
    public String toString() {
        return BearerTokenCredential.class.getSimpleName() + "[" + describeToken() + "]";
    }
}
