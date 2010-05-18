package org.dcache.gplazma;

public interface SessionID {
    /* session IDs should be comparable, for practical reasons.
     * Note that some comparable types like java.sql.time are not comparable to
     * themselves but to a supertype (in the example that is java.util.Date)
     */
    public <T extends Comparable<? super T>> T getSessionID();
    public <T extends Comparable<? super T>> void setSessionID(T sessID);
}
