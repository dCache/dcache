package org.dcache.gplazma.loader.cli;

import java.io.PrintStream;

import org.dcache.gplazma.loader.PluginMetadata;
import org.dcache.gplazma.loader.PluginRepository;
import org.dcache.gplazma.loader.PluginRepositoryFactory;
import org.dcache.gplazma.loader.XmlResourcePluginRepositoryFactory;
import org.dcache.gplazma.loader.PluginRepository.PluginMetadataProcessor;

/**
 * A command the provides information about plugins discovered by some
 * PluginRepositoryFactory. If no PluginRepositoryFactory is registered, with
 * {@link #setFactory(PluginRepositoryFactory), then {
 * @link XmlResourcePluginRepositoryFactory} is used.
 */
public class ListCommand implements Command {
    private PluginRepositoryFactory _factory =
            new XmlResourcePluginRepositoryFactory();
    private PrintStream _out = System.out;

    @Override
    public int run( String[] args) {
        if( args.length > 1) {
            throw new IllegalArgumentException("List takes zero or one arguments");
        }

        if( args.length == 1 && args[0] != "-l") {
            throw new IllegalArgumentException("Only -l is a valid argument");
        }

        PluginMetadataProcessor listProcessor;

        if( args.length == 1) {
            listProcessor = new DetailListPlugins();
        } else {
            listProcessor = new SimpleListPlugins();
        }

        PluginRepository repository = _factory.newRepository();
        repository.processPluginsWith( listProcessor);
        return 0;
    }

    /**
     * Emit a simple list of discovered plugins: the shortest name first and
     * all aliases as a comma-separated list inside parentheses
     */
    private class SimpleListPlugins implements PluginMetadataProcessor {
        @Override
        public void process( PluginMetadata plugin) {
            String shortestName = plugin.getShortestName();
            boolean firstAlias = true;
            StringBuilder sb = new StringBuilder();
            sb.append( shortestName);
            for( String name : plugin.getPluginNames()) {
                if( name.equals( shortestName))
                    continue;
                if( firstAlias) {
                    sb.append( " (");
                } else {
                    sb.append( ",");
                }
                sb.append( name);
                firstAlias = false;
            }
            if( !firstAlias) {
                sb.append( ")");
            }
            _out.println( sb.toString());
        }
    }

    /**
     * Emit a more detailed list of information about plugins.
     */
    private class DetailListPlugins implements PluginMetadataProcessor {
        @Override
        public void process( PluginMetadata plugin) {
            _out.println( "Plugin:");
            _out.println( "    Class: " + plugin.getPluginClass().getName());
            StringBuilder sb = new StringBuilder();
            sb.append( "    Name: ");
            boolean isFirstName = true;
            for( String name : plugin.getPluginNames()) {
                if( !isFirstName) {
                    sb.append( ",");
                }
                sb.append( name);
                isFirstName = false;
            }
            _out.println( sb.toString());
            _out.println( "    Shortest name: " + plugin.getShortestName());
            if( plugin.getDefaultControl() != null) {
                _out.println( "    Default-control: " +
                              plugin.getDefaultControl());
            }
        }
    }

    @Override
    public void setFactory( PluginRepositoryFactory factory) {
        _factory = factory;
    }

    @Override
    public void setOutput( PrintStream out) {
        _out = out;
    }
}
