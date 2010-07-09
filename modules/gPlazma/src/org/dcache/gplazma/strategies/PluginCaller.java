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
     * @throws org.dcache.gplazma.AuthenticationException
     */
    public void call(T plugin) throws AuthenticationException;
}
