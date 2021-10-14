package org.dcache.util;

import java.util.Map;
import org.slf4j.MDC;

/**
 * The class emulates the Nested Diagnostic Context of Log4j.
 * <p>
 * Besides providing static methods for working with the NDC, the class can be instantiated to
 * capture the state of the NDC.
 */
public class NDC {

    /* Internally the class uses the MDC.  Two MDC keys are used: One
     * to hold the NDC in string form, and another to hold a comma
     * separated list of positions in the NDC string indicating the
     * boundaries.
     */
    public static final String KEY_NDC = "org.dcache.ndc";
    public static final String KEY_POSITIONS = "org.dcache.ndc.positions";

    private final String _ndc;
    private final String _positions;

    public NDC(String ndc, String positions) {
        _ndc = ndc;
        _positions = positions;
    }

    public String getNdc() {
        return _ndc;
    }

    public String getPositions() {
        return _positions;
    }

    /**
     * Wrapper around <code>MDC.put</code> and
     * <code>MDC.remove</code>. <code>value</code> is allowed to be
     * null.
     */
    private static void setMdc(String key, String value) {
        if (value != null) {
            MDC.put(key, value);
        } else {
            MDC.remove(key);
        }
    }

    /**
     * Clear any nested diagnostic information if any.
     */
    public static void clear() {
        MDC.remove(KEY_NDC);
        MDC.remove(KEY_POSITIONS);
    }

    /**
     * Returns the nested diagnostic context for the current thread.
     */
    public static NDC cloneNdc() {
        return new NDC(MDC.get(KEY_NDC), MDC.get(KEY_POSITIONS));
    }

    /**
     * Replace the nested diagnostic context.
     */
    public static void set(NDC ndc) {
        setMdc(KEY_NDC, ndc.getNdc());
        setMdc(KEY_POSITIONS, ndc.getPositions());
    }

    /**
     * Push new diagnostic context information for the current thread.
     */
    public static void push(String message) {
        String ndc = MDC.get(KEY_NDC);
        if (ndc == null) {
            MDC.put(KEY_NDC, message);
            MDC.put(KEY_POSITIONS, "0");
        } else {
            MDC.put(KEY_NDC, ndc + ' ' + message);
            MDC.put(KEY_POSITIONS, MDC.get(KEY_POSITIONS) + ',' + ndc.length());
        }
    }

    /**
     * Removes the diagnostic context information pushed the last. Clients should call this method
     * before leaving a diagnostic context.
     */
    public static String pop() {
        String top = null;
        String ndc = MDC.get(KEY_NDC);
        if (ndc != null) {
            String positions = MDC.get(KEY_POSITIONS);
            int pos = positions.lastIndexOf(',');
            if (pos == -1) {
                top = ndc;
                MDC.remove(KEY_NDC);
                MDC.remove(KEY_POSITIONS);
            } else {
                int offset = Integer.parseInt(positions.substring(pos + 1));
                top = ndc.substring(offset + 1);
                MDC.put(KEY_NDC, ndc.substring(0, offset));
                MDC.put(KEY_POSITIONS, positions.substring(0, pos));
            }
        }

        return top;
    }

    public static String ndcFromMdc(Map<String, String> mdc) {
        return mdc.get(KEY_NDC);
    }
}
