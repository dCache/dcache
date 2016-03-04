package org.dcache.gplazma.loader;

/**
 * The XmlResourcePluginLoader is a {@link PluginLoader} that allows
 * instantiation of plugins that have been discovered from XML files stored
 * as resources in the class-path at <tt>META-INF/gplazma-plugins.xml</tt>.
 */
public class XmlResourcePluginLoader extends AbstractPluginLoader {

    private final PluginRepositoryFactory _repositoryFactory =
            new XmlResourcePluginRepositoryFactory();

    public static PluginLoader newPluginLoader() {
        return new SafePluginLoaderDecorator(new XmlResourcePluginLoader());
    }

    private XmlResourcePluginLoader() {
        /*
         * Prevents instantiating the PluginLoader directly: use the
         * newPluginLoader() static method instead.
         */
    }

    @Override
    PluginRepositoryFactory getRepositoryFactory() {
        return _repositoryFactory;
    }
}
