package org.dcache.gplazma.strategies;

import java.security.Principal;
import java.util.Set;

import org.dcache.gplazma.AuthenticationException;
import org.dcache.gplazma.monitor.LoginMonitor;
import org.dcache.gplazma.plugins.GPlazmaMappingPlugin;

/**
 * Implementing classes will use a (combination of) GPlazmaMappingPlugins to
 * perform mapping and reverse-mapping between principals.
 *
 */
public interface MappingStrategy
                 extends GPlazmaStrategy<GPlazmaMappingPlugin> {

    void map(LoginMonitor monitor, Set<Principal> principals)
                throws AuthenticationException;
}
