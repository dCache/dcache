package org.dcache.gplazma.strategies;

import org.dcache.gplazma.AuthenticationException;
import org.dcache.gplazma.plugins.GPlazmaPlugin;
/**
 *
 * @author timur
 */
public interface PluginCaller<T extends GPlazmaPlugin> {
    /**
     *
     * @param plugin
     * @return true if the call is successful (the csll is sufficient for
     * continuation with the next step, i.e. in case of mapping,
     * all the required authorized principals are present ), false otherwise
     * @throws org.dcache.gplazma.AuthenticationException
     */
    public boolean call(T plugin) throws AuthenticationException;
}
