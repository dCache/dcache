package org.dcache.gplazma.strategies;

import java.util.List;

import org.dcache.gplazma.plugins.GPlazmaPlugin;

public interface GPlazmaStrategy<T extends GPlazmaPlugin> {
    public void setPlugins(List<GPlazmaPluginService<T>> plugins);
}
