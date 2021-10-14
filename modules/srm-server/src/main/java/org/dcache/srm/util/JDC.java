package org.dcache.srm.util;

import static com.google.common.base.Strings.nullToEmpty;

import java.util.Base64;
import java.util.Deque;
import java.util.LinkedList;
import java.util.concurrent.atomic.AtomicLong;
import org.dcache.util.NDC;
import org.slf4j.MDC;

/**
 * The SRM Job/request Diagnostic Context, a utility class for working with dCache NDC.
 * <p>
 * The class serves two purposes:
 * <p>
 * - It contains a number of static methods for manipulating the NDC.
 * <p>
 * - JDC instances capture the Job related NDC values. These can be applied to other threads. This
 * is useful when using worker threads that should inherit the context of the task creation point.
 * <p>
 * This class provides AutoCloseable, which allows the try-with-resource pattern; for example, to
 * temporarily restore a captured context (e.g., when processing scheduled activity from
 * thread-pool):
 * <p>
 * try (JDC ignored = otherJdc.apply()) { // logging now with otherJdc context } // logging now with
 * origin context
 * <p>
 * or, when creating a new session ID:
 * <p>
 * try (JDC ignored = JDC.createSession("my-session")) { // logging now with session. } // logging
 * with original context
 * <p>
 * or simply to roll-back any potential changes to the context:
 * <p>
 * try (JDC ignored = new JDC()) { // activity that could modify JDC } // logging now with original
 * context
 */
public class JDC implements AutoCloseable {

    // FIXME this value must be the same as dmg.cells.nucleus.CDC.MDC_SESSION
    // as the mapping JDC --> CDC currently requires this coincidence.
    public static final String MDC_SESSION = "cells.session";

    private static final String _epoc = createEpocString() + ":";
    private static final AtomicLong _id = new AtomicLong();

    private final NDC _ndc;
    private final String _session;

    /**
     * Captures the cells diagnostic context of the calling thread.
     */
    public JDC() {
        _session = getSession();
        _ndc = NDC.cloneNdc();
    }

    /**
     * Wrapper around <code>MDC.put</code> and
     * <code>MDC.remove</code>. <code>value</code> is allowed to e
     * null.
     */
    private static void setMdc(String key, String value) {
        if (value != null) {
            MDC.put(key, value);
        } else {
            MDC.remove(key);
        }
    }


    @Override
    public void close() {
        apply();
    }

    /**
     * Applies the saved context to the calling thread.
     */
    public JDC apply() {
        JDC jdc = new JDC();
        setMdc(MDC_SESSION, _session);
        NDC.set(_ndc);
        return jdc;
    }

    /**
     * Returns this session's identifier.  The value is bound to the current thread.
     */
    public static String getSession() {
        return MDC.get(MDC_SESSION);
    }

    /**
     * Sets a session identifier.  If session has not been set then the session is pushed onto the
     * NDC.  If the session has already been set then the existing NDC value is updated with the new
     * session identifier.
     *
     * @param id Session identifier.
     */
    public static void setSession(String id) {
        Deque<String> items = new LinkedList<>();

        String oldId = MDC.get(MDC_SESSION);
        if (oldId != null) {
            String popped = NDC.pop();

            while (popped != null && !popped.equals(oldId)) {
                items.push(popped);
                popped = NDC.pop();
            }
        }

        NDC.push(id);

        while (items.peek() != null) {
            NDC.push(items.pop());
        }

        setMdc(MDC_SESSION, id);
    }

    /**
     * Creates a session identifier and pushes this onto the NDC. The ID has two parts: {@literal
     * <SRM REQUEST>:<SRM OPERATION>}, where {@literal <SRM REQUEST>} identifies a specific SOAP
     * interaction and {@literal <SRM OPERATION>} identifies an operation that may span multiple
     * {@literal <SRM REQUEST>}s.  For example, uploading a single file will have exactly one
     * {@literal <SRM OPERATION>} but multiple {@literal <SRM REQUEST>}s, consisting of exactly one
     * {@literal srmPrepareToPut} request, zero or more {@literal srmStatusOfPutRequest} requests
     * and exactly one {@literal srmPutDone} request.
     * <p>
     * By making this distinction explicit in the Session-ID format, an admin may search for session
     * IDs that match the complete session-ID when looking for information about a specific SRM
     * request, or for the {@literal <SRM OPERATION>}-part when looking for all SRM requests for a
     * specific SRM operation.
     *
     * @param request Information identifying the SRM request
     * @return a JDC capturing the previous context
     */
    public static JDC createSession(String request) {
        JDC current = new JDC();
        setSession(_epoc + Long.toString(_id.incrementAndGet()) + ":" + request);
        return current;
    }

    /**
     * Add {@literal <SRM OPERATION>} information to the session ID. The new session ID is a
     * combination of the existing session ID (if any) and the suffix.  The NDC is updated
     * accordingly.
     */
    public static void appendToSession(String suffix) {
        setSession(nullToEmpty(getSession()) + ":" + suffix);
    }

    private static String createEpocString() {
        long time = System.currentTimeMillis();
        byte hash1 = (byte) (time ^ (time >>> 8) ^ (time >>> 24) ^ (time >>> 40)
              ^ (time >>> 56));
        byte hash2 = (byte) ((time >>> 16) ^ (time >>> 32) ^ (time >>> 48));
        String id = Base64.getEncoder().encodeToString(new byte[]{hash1, hash2});
        int idx = id.indexOf('=');
        return idx == -1 ? id : id.substring(0, idx);
    }
}
