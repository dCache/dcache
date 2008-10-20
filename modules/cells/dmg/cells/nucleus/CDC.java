package dmg.cells.nucleus;

import java.util.Stack;

import org.apache.log4j.MDC;
import org.apache.log4j.NDC;

/**
 * The Cell Diagnostic Context, a utility class for working with the
 * Log4j NDC and MDC.
 *
 * Notice that the MDC is automatically inherited by child threads
 * upon creation. Thus the domain and cell name is inherited. The same
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
 * - CDC instances capture the Cells related MDC values and the NDC.
 *   These can be apply to other threads. This is useful when using
 *   worker threads that should inherit the context of the task
 *   creation point.
 *
 */
public class CDC
{
    public final static String MDC_DOMAIN = "cells.domain";
    public final static String MDC_CELL = "cells.cell";

    private final Stack _ndc;
    private final Object _cell;
    private final Object _domain;

    /**
     * Captures the cells diagnostic context of the calling thread.
     */
    public CDC()
    {
        _cell = MDC.get(MDC_CELL);
        _domain = MDC.get(MDC_DOMAIN);
        _ndc = NDC.cloneStack();
    }

    /**
     * Wrapper around <code>MDC.put</code> and
     * <code>MDC.remove</code>. <code>value</code> is allowed to e
     * null.
     */
    static private void setMdc(String key, Object value)
    {
        if (value != null)
            MDC.put(key, value);
        else
            MDC.remove(key);
    }

    /**
     * Applies the cells diagnostic context to the calling thread.
     */
    public void apply()
    {
        setMdc(MDC_DOMAIN, _domain);
        setMdc(MDC_CELL, _cell);
        NDC.clear();
        NDC.inherit(_ndc);
    }

    /**
     * Setup the cell diagnostic context of the calling
     * thread. Threads created from the calling thread automatically
     * inherit this information.
     */
    static public void setCellsContext(CellNucleus cell)
    {
        MDC.put(MDC_CELL, cell.getCellName());
        MDC.put(MDC_DOMAIN, cell.getCellDomainName());
    }

    /**
     * Returns the message description added to the NDC as part of the
     * message related diagnostic context.
     */
    static protected String getMessageContext(CellMessage envelope)
    {
        StringBuilder s = new StringBuilder();

        s.append(envelope.getSourceAddress().getCellName());

        Object msg = envelope.getMessageObject();
        if (msg instanceof HasDiagnosticContext) {
            String context =
                ((HasDiagnosticContext) msg).getDiagnosticContext();
            s.append(' ').append(context);
        }

        return s.toString();
    }

    /**
     * Setup message related diagnostic context for the calling
     * thread. Adds information about a message to the MDC and NDC.
     *
     * @see clearMessageContext
     */
    static public void setMessageContext(CellMessage envelope)
    {
        NDC.push(getMessageContext(envelope));
    }

    /**
     * Clears the diagnostic context entries added by
     * <code>setMessageContext</code>.  For this to work, the NDC has
     * to be in the same state as when <code>setMessageContext</code>
     * returned.
     *
     * @see setMessageContext
     */
    static public void clearMessageContext()
    {
        NDC.pop();
    }

    /**
     * Clears all cells related MDC entries and the NDC.
     */
    static public void clear()
    {
        MDC.remove(MDC_DOMAIN);
        MDC.remove(MDC_CELL);
        NDC.clear();
    }
}