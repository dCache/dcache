package org.dcache.boot;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.Sets;

import java.io.File;
import java.io.IOException;
import java.io.LineNumberReader;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Objects;
import java.util.Set;

import org.dcache.util.ConfigurationProperties;

import static com.google.common.collect.Iterables.transform;
import static org.dcache.boot.Properties.*;

public class LayoutBuilder
{
    private static final Function<File, String> QUOTE_FILE =
            new Function<File, String>()
            {
                @Override
                public String apply(File input)
                {
                    return '"' + input.getPath() + '"';
                }
            };

    private final Set<File> _sourceFiles = Sets.newHashSet();
    private final Set<File> _sourceDirectories = Sets.newHashSet();
    private ConfigurationProperties.ProblemConsumer problemConsumer =
            new SilentProblemConsumer();

    public LayoutBuilder setProblemConsumer(ConfigurationProperties.ProblemConsumer problemConsumer)
    {
        this.problemConsumer = problemConsumer;
        return this;
    }

    public Layout build() throws IOException, URISyntaxException
    {
        return loadLayout(loadConfiguration());
    }

    private ConfigurationProperties loadSystemProperties()
            throws UnknownHostException
    {
        ConfigurationProperties config =
                new ConfigurationProperties(System.getProperties());
        InetAddress localhost = InetAddress.getLocalHost();
        config.setProperty(PROPERTY_HOST_NAME,
                localhost.getHostName().split("\\.")[0]);
        config.setProperty(PROPERTY_HOST_FQDN,
                localhost.getCanonicalHostName());
        return config;
    }

    private ConfigurationProperties loadConfigurationFromPath(
            ConfigurationProperties defaults, File path, ConfigurationProperties.UsageChecker usageChecker)
            throws IOException
    {
        ConfigurationProperties config = new ConfigurationProperties(defaults, usageChecker);
        if (path.isFile()) {
            _sourceFiles.add(path);
            config.loadFile(path);
        } else if (path.isDirectory()) {
            _sourceDirectories.add(path);
            File[] files = path.listFiles();
            if (files != null) {
                Arrays.sort(files);
                for (File file: files) {
                    if (file.isFile() && file.getName().endsWith(".properties")) {
                        _sourceFiles.add(file);
                        config.loadFile(file);
                    }
                }
            }
        }
        return config;
    }

    private ConfigurationProperties loadConfigurationByProperty(
            ConfigurationProperties defaults, String property, ConfigurationProperties.UsageChecker usageChecker)
            throws IOException
    {
        ConfigurationProperties config = defaults;
        String paths = config.getValue(property);
        if (paths != null) {
            for (String path: paths.split(PATH_DELIMITER)) {
                config = loadConfigurationFromPath(config, new File(path), usageChecker);
            }
        }
        return config;
    }

    /**
     * Loads plugins in a plugin directory.
     *
     * A plugin directory contains a number of plugins. Each plugin is
     * stored in a sub-directory containing that one plugin.
     */
    private ConfigurationProperties loadPlugins(
            ConfigurationProperties defaults, File directory)
            throws IOException
    {
        ConfigurationProperties config = defaults;
        _sourceDirectories.add(directory);
        File[] files = directory.listFiles();
        if (files != null) {
            for (File file: files) {
                if (file.isDirectory()) {
                    config = loadConfigurationFromPath(config, file, new ConfigurationProperties.UniversalUsageChecker());
                }
            }
        }
        return config;
    }

    private ConfigurationProperties loadConfiguration()
            throws UnknownHostException, IOException, URISyntaxException
    {
        /* Configuration properties are loaded from several
         * sources, starting with importing Java system
         * properties...
         */
        ConfigurationProperties config = loadSystemProperties();
        config.setProblemConsumer(problemConsumer);

        /* ... and a chain of properties files. */
        config = loadConfigurationByProperty(config, PROPERTY_DEFAULTS_PATH, new ConfigurationProperties.UniversalUsageChecker());
        for (String dir: getPluginDirs()) {
            config = loadPlugins(config, new File(dir));
        }
        config = loadConfigurationByProperty(config, PROPERTY_SETUP_PATH, new DcacheConfigurationUsageChecker());

        return config;
    }

    private Layout loadLayout(ConfigurationProperties config)
            throws IOException, URISyntaxException
    {
        String path = config.getValue(PROPERTY_DCACHE_LAYOUT_URI);
        if (path == null) {
            throw new IOException("Undefined property: " + PROPERTY_DCACHE_LAYOUT_URI);
        }
        URI uri = new URI(path);
        Layout layout = new Layout(config);
        layout.load(uri);

        if (Objects.equals(uri.getScheme(), "file")) {
            _sourceFiles.add(new File(uri.getPath()));
        } else {
            layout.properties().setProperty(PROPERTY_DCACHE_CONFIG_CACHE, "false");
        }
        layout.properties().setProperty(PROPERTY_DCACHE_CONFIG_FILES,
                Joiner.on(" ").join(transform(_sourceFiles, QUOTE_FILE)));
        layout.properties().setProperty(PROPERTY_DCACHE_CONFIG_DIRS,
                Joiner.on(" ").join(transform(_sourceDirectories, QUOTE_FILE)));
        return layout;
    }

    /**
     * Returns the top-level plugin directory.
     *
     * To allow the plugin directory to be configurable, we first have
     * to load all the configuration files without the plugins.
     */
    private String[] getPluginDirs()
            throws IOException, URISyntaxException
    {
        ConfigurationProperties config = loadSystemProperties();
        config.setProblemConsumer(new SilentProblemConsumer());
        config = loadConfigurationByProperty(config, PROPERTY_DEFAULTS_PATH, new ConfigurationProperties.UniversalUsageChecker());
        config = loadConfigurationByProperty(config, PROPERTY_SETUP_PATH, new DcacheConfigurationUsageChecker());
        config = loadLayout(config).properties();
        String dir = config.getValue(PROPERTY_PLUGIN_PATH);
        return (dir == null) ? new String[0] : dir.split(PATH_DELIMITER);
    }

    private static class SilentProblemConsumer implements ConfigurationProperties.ProblemConsumer
    {
        @Override
        public void setFilename(String name)
        {
        }

        @Override
        public void setLineNumberReader(LineNumberReader reader)
        {
        }

        @Override
        public void error(String message)
        {
        }

        @Override
        public void warning(String message)
        {
        }

        @Override
        public void info(String message)
        {
        }
    }
}
