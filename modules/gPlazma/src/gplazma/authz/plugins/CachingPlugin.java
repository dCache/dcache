package gplazma.authz.plugins;
import org.apache.log4j.Logger;

/**
 * Created by IntelliJ IDEA.
 * User: tdh
 * Date: Sep 16, 2008
 * Time: 3:47:17 PM
 * To change this template use File | Settings | File Templates.
 */
public abstract class CachingPlugin extends AuthorizationPlugin {
    private static final Logger logger = Logger.getLogger(CachingPlugin.class);

    long cache_lifetime=0;

    public CachingPlugin(long authRequestID) {
        super(authRequestID);
    }

    public void setCacheLifetime(String lifetime_str) {
        if(lifetime_str==null || lifetime_str.length()==0) return;
        try {
            setCacheLifetime(Long.decode(lifetime_str).longValue()*1000);
        } catch (NumberFormatException nfe) {
            logger.error("Could not format saml-vo-mapping-cache-lifetime=" + lifetime_str + " as long integer.");
        }
    }

    public void setCacheLifetime(long lifetime) {
        cache_lifetime = lifetime;
    }

    public long getCacheLifetime() {
        return cache_lifetime;
    }
}
