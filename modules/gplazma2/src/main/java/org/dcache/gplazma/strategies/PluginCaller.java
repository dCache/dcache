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
     * @throws AuthenticationException
     */
    void call(GPlazmaPluginService<T> plugin) throws AuthenticationException;
}
