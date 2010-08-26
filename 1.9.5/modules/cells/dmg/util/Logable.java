package dmg.util;

/**
 * Consider to use log4j Logger. Please have a look at:
 * http://trac.dcache.org/trac.cgi/wiki/LogFacility for more information
 */

@Deprecated
public interface Logable {
    /**
     * Should log information only
     */
    public void log(String message);

    /**
     * Should log errors
     */
    public void elog(String message);

    /**
     * Should log unrecoverable problems
     */
    public void plog(String message);

}
