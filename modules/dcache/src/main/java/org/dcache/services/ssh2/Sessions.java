package org.dcache.services.ssh2;

import com.google.common.primitives.Longs;
import java.util.Base64;
import org.apache.sshd.common.session.Session;

/**
 * Utility class for handling SSH sessions.
 */
public class Sessions {

    private static final String SESSION_ID_PREFIX = createEpocString() + ":";

    private Sessions() {
        // Prevent instantiation: this is a utility class.
    }

    private static String createEpocString() {
        long time = System.currentTimeMillis();
        byte hash1 = (byte) (time ^ (time >>> 8) ^ (time >>> 24) ^ (time >>> 40)
              ^ (time >>> 56));
        byte hash2 = (byte) ((time >>> 16) ^ (time >>> 32) ^ (time >>> 48));
        return Base64.getEncoder().withoutPadding().encodeToString(new byte[]{hash1, hash2});
    }

    /**
     * Provide a String that (with very high likelihood) is unique for this network connection.
     *
     * @param session The Session associated with this network connection
     * @return a short unique string
     */
    public static String connectionId(Session session) {
        long sessionId = session.getIoSession().getId();
        byte[] rawId = Longs.toByteArray(sessionId);
        String shortId = Base64.getEncoder().withoutPadding().encodeToString(rawId);

        int idx = 0;
        while (idx < shortId.length() && shortId.indexOf('A', idx) == idx) {
            idx++;
        }

        return SESSION_ID_PREFIX + shortId.substring(idx);
    }
}
