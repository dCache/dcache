package org.dcache.srm.util;

import java.util.Iterator;
import java.util.Stack;
import java.io.Serializable;
import org.apache.log4j.MDC;
import org.apache.log4j.NDC;
import org.dcache.srm.scheduler.Job;




	/**
	 * The SRM Job/request Diagnostic Context, a utility class for working with the
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
	 *   These can be apply to other threads. This is useful when using
	 *   worker threads that should inherit the context of the task
	 *   creation point.
	 *
	 */
	public class JDC
	{

	    public final static String MDC_SESSION = "srm.session";
	    public final static String MDC_SCHEDULER = "srm.scheduler";
	    public final static String MDC_DCACHE_SESSION = "cells.session";

	    private static long _last;
            private final static long ThreeYears = 3*365*24*3600*1000;

	    private final Stack _ndc;
	    private final Object _session;
            private final Object _scheduler;
            private final String _dcacheSession;

            static class DCacheSessionHelper {

                private static final String DELIM = ":";

                public static void push(String sessionComp){
                   String currentValue = get();
                   if ( currentValue == null )
                       set(sessionComp);
                   else {
                       set(currentValue +DELIM+sessionComp);
                   }
                }

                public static void pop(){
                   String currentValue = get();
                   if ( currentValue != null ){
                      int pos = currentValue.lastIndexOf(DELIM);
                      if ( pos == -1 ){
                         currentValue = null;
                      }else{
                         currentValue = currentValue.substring(0,pos);
                      }
                      set(currentValue);
                   }
                }

                public static void set(String session)
                {
                   setMdc(MDC_DCACHE_SESSION,session);
                }

                public static String get(){
                   String dCacheSession = (String)MDC.get(MDC_DCACHE_SESSION);
                   return dCacheSession;
                }

            }

	    /**
	     * Captures the cells diagnostic context of the calling thread.
	     */
	    public JDC()
	    {
               _session = MDC.get(MDC_SESSION);
               _scheduler = MDC.get(MDC_SCHEDULER);
               _dcacheSession = DCacheSessionHelper.get();
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
	     * Applies the srm job diagnostic context to the calling thread.
	     * May only be called once. Equivalent to calling
	     * <code>apply(false)</code>.
	     */
	    public void apply()
	    {
	        apply(false);
	    }

	    /**
	     * Applies the srm job diagnostic context to the calling thread.  If
	     * <code>clone</code> is false, then the <code>apply</code> can
	     * only be called once for this JDC. If <code>clone</code> is
	     * true, then the JDC may be applied several times, however the
	     * operation is more expensive.
	     *
	     * @param clone whether to apply a clone of the NDC stack
	     */
	    public void apply(boolean clone)
	    {
	        setMdc(MDC_SESSION, _session);
	        setMdc(MDC_SCHEDULER,_scheduler);
	        NDC.clear();
	        if ( _ndc != null )
	           NDC.inherit(clone ? (Stack) _ndc.clone() : _ndc);
	        DCacheSessionHelper.set(_dcacheSession);
	    }

	    /**
	     * Returns the session identifier stored in the MDC of the calling
	     * thread.
	     */
	    static public Object getSession()
	    {
	        return MDC.get(MDC_SESSION);
	    }

	    /**
	     * Sets the session in the JDC for the calling thread.
	     *
	     * @param session Session identifier.
	     */
	    static public void setSession(Object session)
	    {
              setMdc(MDC_SESSION, session);
              DCacheSessionHelper.set(session.toString());
	    }

	    /**
         * Creates the session in the JDC for the calling thread.
         *
         * @param prefix Prefix for the newly created session identifier.
         */
	    static public void createSession(String prefix)
	    {
	        setSession(prefix+createUniq());
	    }


	    /**
	     * Returns the message description added to the NDC as part of the
	     * message related diagnostic context.
	     */
	    static protected String getJobContext(Job job)
	    {
	        StringBuilder s = new StringBuilder();

	        Object jid = job.getId();
	        if (jid != null) {
	            s.append("jid=").append(jid.toString());
	        }

	        return s.toString();
	    }


	    static public void setJobContext(Job job)
	    {
	        NDC.push(getJobContext(job));
	        DCacheSessionHelper.push(job.getId().toString());
	    }

	    /**
	     * Clears the diagnostic context entries added by
	     * <code>setJobContext</code>.  For this to work, the NDC has
	     * to be in the same state as when <code>setJobContext</code>
	     * returned.
	     *
	     * @see setJobContext
	     */
	    static public void clearJobContext()
	    {
	        NDC.pop();
	        DCacheSessionHelper.pop();
	    }

            public static void setSchedulerContext(Object schedulerContext) {
	        MDC.put(MDC_SCHEDULER, schedulerContext);
            }

            public static String createUniq() {
                _last = Math.max(_last + 1, System.currentTimeMillis());
                return Long.toString(_last % ThreeYears );
            }
}
