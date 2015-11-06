package org.dcache.gplazma.loader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URL;
import java.util.Enumeration;
import java.util.List;
import java.util.ArrayList;

/**
 * Use the <tt>META-INF/gplazma-plugins.xml</tt> file(s) available from the thread's
 * class-loader to build a PluginRepository.
 */
public class XmlResourcePluginRepositoryFactory implements
        PluginRepositoryFactory {

    private static final Logger LOGGER =
        LoggerFactory.getLogger( XmlResourcePluginRepositoryFactory.class);

    public static final String RESOURCE_PATH =
        "META-INF/gplazma-plugins.xml";


    @Override
    public PluginRepository newRepository() {
        List<URL> xmlResources = findXmlResources();
        PluginRepository repository = new PluginRepository();
        addPlugins( repository, xmlResources);
        return repository;
    }

    private List<URL> findXmlResources() {
        List<URL> results = new ArrayList<>();

        ClassLoader classLoader =
                Thread.currentThread().getContextClassLoader();

        try {
            Enumeration<URL> resources =
                    classLoader.getResources( RESOURCE_PATH);

            while (resources.hasMoreElements()) {
                results.add( resources.nextElement());
            }
        } catch (IOException e) {
            LOGGER.error( "Unable to locate plugin metadata", e);
        }

        return results;
    }

    private void addPlugins( PluginRepository repository, List<URL> xmlResources) {
        for( URL xmlLocation : xmlResources) {
            tryAddingPluginsFromXml( repository, xmlLocation);
        }
    }

    private void tryAddingPluginsFromXml( PluginRepository repository,
                                          URL xmlLocation) {
        try {
            addPluginsFromXml( repository, xmlLocation);
        } catch (IOException e1) {
            LOGGER.error( "Unable read XML data from {}", xmlLocation
                    .toExternalForm());
        }
    }

    private void addPluginsFromXml( PluginRepository repository, URL xmlLocation)
            throws IOException {
        InputStream is = xmlLocation.openStream();
        Reader in = new BufferedReader( new InputStreamReader( is));
        XmlParser parser = new XmlParser( in);
        parser.parse();

        for( PluginMetadata plugin : parser.getPlugins()) {
            repository.addPlugin( plugin);
        }
    }
}
