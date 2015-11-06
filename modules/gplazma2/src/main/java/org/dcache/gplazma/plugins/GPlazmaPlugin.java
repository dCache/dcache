package org.dcache.gplazma.plugins;

public interface GPlazmaPlugin
{
    default void start() throws Exception
    {
    }

    default void stop() throws Exception
    {
    }
}
