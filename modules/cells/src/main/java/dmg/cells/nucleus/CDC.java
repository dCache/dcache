package dmg.cells.nucleus;

import org.slf4j.MDC;

import org.dcache.util.NDC;

/**
 * The Cell Diagnostic Context, a utility class for working with the
 * Log4j NDC and MDC.
 *
 * Notice that the MDC is automatically inherited by child threads
 * upon creation. Thus the domain, cell name, and session identifier
 * is inherited. The same is not true for the NDC, which needs to be
 * explicitly copied. Special care must be taken when using shared
 * thread pools, as this can span several cells. For those the MDC
 * should be initialised per task, not per thread.
 *
 * The class serves two purposes:
 *
 * - It contains a number of static methods for manipulating the MDC
 *   and NDC.
 *
 * - CDC instances capture the Cells related MDC values and the NDC.
 *   These can be applied to other threads. This is useful when using
 *   worker threads that should inherit the context of the task
 *   creation point.
 *
 *   CDC implements AutoCloseable and will restore the captured values
 *   when closed(). This allows the following useful patterns:
 *
 *     try (CDC ignored = new CDC()) {
 *       // temporarily modify the CDC - it will auto restore
 *     }
 *
 *   and
 *
 *     try (CDC ignored = cdc.restore()) {
 *         // temporarily use a previously captured cdc
 *     }
 */
public class CDC implements AutoCloseable
{
    public static final String MDC_DOMAIN = "cells.domain";
    public static final String MDC_CELL = "cells.cell";
    public static final String MDC_SESSION = "cells.session";

    private final NDC _ndc;
    private final String _session;
    private final String _cell;
    private final String _domain;

    /**
     * Captures the cells diagnostic context of the calling thread.
     */
    public CDC()
    {
        _session = MDC.get(MDC_SESSION);
        _cell = MDC.get(MDC_CELL);
        _domain = MDC.get(MDC_DOMAIN);
        _ndc = NDC.cloneNdc();
    }

    /**
     * Wrapper around <code>MDC.put</code> and
     * <code>MDC.remove</code>. <code>value</code> is allowed to e
     * null.
     */
    private static void setMdc(String key, String value)
    {
        if (value != null) {
            MDC.put(key, value);
        } else {
            MDC.remove(key);
        }
    }

    private void apply()
    {
        setMdc(MDC_DOMAIN, _domain);
        setMdc(MDC_CELL, _cell);
        setMdc(MDC_SESSION, _session);
        if (_ndc == null) {
            NDC.clear();
        } else {
            NDC.set(_ndc);
        }
    }

    @Override
    public void close()
    {
        apply();
    }

    /**
     * Restore the cells diagnostic context to the calling thread. The old
     * diagnostic context is captured and returned.
     */
    public CDC restore()
    {
        CDC cdc = new CDC();
        apply();
        return cdc;
    }

    /**
     * Execute the given runnable within the context of this CDC.
     */
    public void execute(Runnable r)
    {
        String session = MDC.get(MDC_SESSION);
        String cell = MDC.get(MDC_CELL);
        String domain = MDC.get(MDC_DOMAIN);
        NDC ndc = NDC.cloneNdc();
        try {
            apply();
            r.run();
        } finally {
            setMdc(MDC_DOMAIN, domain);
            setMdc(MDC_CELL, cell);
            setMdc(MDC_SESSION, session);
            NDC.set(ndc);
        }
    }

    /**
     * Returns the cell name stored in the MDC of the calling
     * thread.
     */
    public static String getCellName()
    {
        return MDC.get(MDC_CELL);
    }

    /**
     * Returns the domain name stored in the MDC of the calling
     * thread.
     */
    public static String getDomainName()
    {
        return MDC.get(MDC_DOMAIN);
    }

    /**
     * Returns the session identifier stored in the MDC of the calling
     * thread.
     */
    public static String getSession()
    {
        return MDC.get(MDC_SESSION);
    }

    /**
     * Sets the session in the MDC for the calling thread.
     *
     * @param session Session identifier.
     */
    public static void setSession(String session)
    {
        setMdc(MDC_SESSION, session);
    }

    /**
     * Setup the cell diagnostic context of the calling
     * thread. Threads created from the calling thread automatically
     * inherit this information. The old diagnostic context is
     * captured and returned.
     */
    public static CDC reset(CellNucleus cell)
    {
        return reset(cell.getCellName(), cell.getCellDomainName());
    }

    public static CDC reset(CellAddressCore address)
    {
        return reset(address.getCellName(), address.getCellDomainName());
    }

    /**
     * Discover the current thread's cell and reset its CDC.  If the current
     * thread was created outside of a cell's ThreadGroup then it is "inherited"
     * by the System cell.
     */
    public static void discoverAndReset()
    {
        CellNucleus nucleus = CellNucleus.findForThread(Thread.currentThread())
                .orElseGet(() -> CellNucleus.getLogTargetForCell(null));
        CDC.reset(nucleus);
    }

    /**
     * Setup the cell diagnostic context of the calling
     * thread. Threads created from the calling thread automatically
     * inherit this information. The old diagnostic context is
     * captured and returned.
     */
    public static CDC reset(String cellName, String domainName)
    {
        CDC cdc = new CDC();
        setMdc(MDC_CELL, cellName);
        setMdc(MDC_DOMAIN, domainName);
        MDC.remove(MDC_SESSION);
        NDC.clear();
        return cdc;
    }

    /**
     * Returns the message description added to the NDC as part of the
     * message related diagnostic context.
     */
    protected static String getMessageContext(CellMessage envelope)
    {
        Object sessionObject = envelope.getSession();
        String session = (sessionObject == null) ? null : sessionObject.toString();
        String cellName = envelope.getSourcePath().getCellName();
        Object msg = envelope.getMessageObject();
        String context = (msg instanceof HasDiagnosticContext) ? ((HasDiagnosticContext) msg).getDiagnosticContext() : null;

        StringBuilder s = new StringBuilder(((session == null) ? 0 : session.length() + 1) +
                                            cellName.length() +
                                            ((context == null) ? 0 : 1 + context.length()));
        if (session != null) {
            s.append(session).append(' ');
        }
        s.append(cellName);
        if (context != null) {
            s.append(' ').append(context);
        }
        return s.toString();
    }

    /**
     * Setup message related diagnostic context for the calling
     * thread. Adds information about a message to the MDC and NDC.
     *
     * @see #clearMessageContext
     */
    public static void setMessageContext(CellMessage envelope)
    {
        Object session = envelope.getSession();
        NDC.push(getMessageContext(envelope));
        setMdc(MDC_SESSION, (session == null) ? null : session.toString());
    }

    /**
     * Clears the diagnostic context entries added by
     * <code>setMessageContext</code>.  For this to work, the NDC has
     * to be in the same state as when <code>setMessageContext</code>
     * returned.
     *
     * @see #setMessageContext
     */
    public static void clearMessageContext()
    {
        MDC.remove(MDC_SESSION);
        NDC.pop();
    }

    /**
     * Clears all cells related MDC entries and the NDC.
     */
    public static void clear()
    {
        MDC.remove(MDC_DOMAIN);
        MDC.remove(MDC_CELL);
        MDC.remove(MDC_SESSION);
        NDC.clear();
    }
}
