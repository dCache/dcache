#set( $symbol_pound = '#' )
#set( $symbol_dollar = '$' )
#set( $symbol_escape = '\' )
package ${package};

import java.util.Properties;

import org.dcache.pool.nearline.spi.NearlineStorage;
import org.dcache.pool.nearline.spi.NearlineStorageProvider;

public class PluginNearlineStorageProvider implements NearlineStorageProvider
{
    @Override
    public String getName()
    {
        return "${name}";
    }

    @Override
    public String getDescription()
    {
        return "${description}";
    }

    @Override
    public NearlineStorage createNearlineStorage(String type, String name)
    {
        return new PluginNearlineStorage(type, name);
    }
}
