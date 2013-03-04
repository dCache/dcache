package org.dcache.srm.util;

import org.slf4j.MDC;

import org.dcache.commons.util.NDC;

/**
 * The SRM Job/request Diagnostic Context, a utility class for working
 * with the Log4j NDC and MDC.
 *
 * Notice that the MDC is automatically inherited by child threads
 * upon creation. Thus the session identifier is inherited. The same
 * is not true for the NDC, which needs to be explicitly
 * copied. Special care must be taken when using shared thread pools,
 * as this can span several cells. For those the MDC should be
 * initialised per task, not per thread.
 *
 * The class serves two purposes:
 *
 * - It contains a number of static methods for manipulating the MDC
 *   and NDC.
 *
 * - JDC instances capture the Job related MDC values and the NDC.
 *   These can be applied to other threads. This is useful when using
 *   worker threads that should inherit the context of the task
 *   creation point.
 */
public class JDC
{
    public final static String MDC_SESSION = "cells.session";

    private final static String DELIM = ":";
    private final static long THREE_YEARS = 3*365*24*3600*1000;

    private static long _last;

    private final NDC _ndc;
    private final String _session;

    /**
     * Captures the cells diagnostic context of the calling thread.
     */
    public JDC()
    {
        _session = getSession();
        _ndc = NDC.cloneNdc();
    }

    /**
     * Wrapper around <code>MDC.put</code> and
     * <code>MDC.remove</code>. <code>value</code> is allowed to e
     * null.
     */
    static private void setMdc(String key, String value)
    {
        if (value != null) {
            MDC.put(key, value);
        } else {
            MDC.remove(key);
        }
    }

    /**
     * Applies the srm job diagnostic context to the calling thread.  If
     * <code>clone</code> is false, then the <code>apply</code> can
     * only be called once for this JDC. If <code>clone</code> is
     * true, then the JDC may be applied several times, however the
     * operation is more expensive.
     */
    public void apply()
    {
        setMdc(MDC_SESSION, _session);
        if (_ndc == null) {
            NDC.clear();
        } else {
            NDC.set(_ndc);
        }
    }

    /**
     * Returns the session identifier stored in the MDC of the calling
     * thread.
     */
    static public String getSession()
    {
        return MDC.get(MDC_SESSION);
    }

    /**
     * Sets the session in the JDC for the calling thread.
     *
     * @param session Session identifier.
     */
    static public void setSession(String session)
    {
        setMdc(MDC_SESSION, session);
    }

    /**
     * Creates the session in the JDC for the calling thread.
     *
     * @param prefix Prefix for the newly created session identifier.
     */
    static public void createSession(String prefix)
    {
        setSession(prefix + createUniq());
        NDC.push(getSession());
    }

    static public String createUniq()
    {
        _last = Math.max(_last + 1, System.currentTimeMillis());
        return Long.toString(_last % THREE_YEARS);
    }

    static public void push(String jid)
    {
        assert getSession() != null;

        setSession(getSession() + DELIM + jid);
        NDC.pop();
        NDC.push(getSession());
    }

    /**
     * Clears all cells related MDC entries and the NDC.
     */
    static public void clear()
    {
        MDC.remove(MDC_SESSION);
        NDC.clear();
    }
}
