package org.dcache.boot;

import static java.util.stream.Collectors.joining;
import static org.dcache.boot.Properties.PATH_DELIMITER;
import static org.dcache.boot.Properties.PROPERTY_DCACHE_CONFIG_CACHE;
import static org.dcache.boot.Properties.PROPERTY_DCACHE_CONFIG_DIRS;
import static org.dcache.boot.Properties.PROPERTY_DCACHE_CONFIG_FILES;
import static org.dcache.boot.Properties.PROPERTY_DCACHE_LAYOUT_URI;
import static org.dcache.boot.Properties.PROPERTY_DCACHE_SCM_STATE;
import static org.dcache.boot.Properties.PROPERTY_DCACHE_VERSION;
import static org.dcache.boot.Properties.PROPERTY_DEFAULTS_PATH;
import static org.dcache.boot.Properties.PROPERTY_HOST_FQDN;
import static org.dcache.boot.Properties.PROPERTY_HOST_NAME;
import static org.dcache.boot.Properties.PROPERTY_PLUGIN_PATH;
import static org.dcache.boot.Properties.PROPERTY_SETUP_PATH;

import com.google.common.collect.Sets;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.LineNumberReader;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Objects;
import java.util.Set;
import org.dcache.util.Version;
import org.dcache.util.configuration.ConfigurationProperties;
import org.dcache.util.configuration.ProblemConsumer;
import org.dcache.util.configuration.UniversalUsageChecker;
import org.dcache.util.configuration.UsageChecker;

public class LayoutBuilder {

    private final Set<File> _sourceFiles = Sets.newHashSet();
    private final Set<File> _sourceDirectories = Sets.newHashSet();
    private ProblemConsumer problemConsumer = new SilentProblemConsumer();

    public LayoutBuilder setProblemConsumer(ProblemConsumer problemConsumer) {
        this.problemConsumer = problemConsumer;
        return this;
    }

    public Layout build() throws IOException, URISyntaxException {
        return loadLayout(loadConfiguration());
    }

    private ConfigurationProperties loadSystemProperties()
          throws UnknownHostException {
        ConfigurationProperties config =
              new ConfigurationProperties(System.getProperties());
        InetAddress localhost = InetAddress.getLocalHost();
        config.setProperty(PROPERTY_HOST_NAME,
              localhost.getHostName().split("\\.")[0]);
        config.setProperty(PROPERTY_HOST_FQDN,
              localhost.getCanonicalHostName());
        config.setProperty(PROPERTY_DCACHE_VERSION,
              Version.of(this).getVersion());
        config.setProperty(PROPERTY_DCACHE_SCM_STATE,
              Version.of(this).getBuild());
        return config;
    }

    private ConfigurationProperties loadConfigurationFromPath(
          ConfigurationProperties defaults, File path, UsageChecker usageChecker)
          throws IOException {
        ConfigurationProperties config = new ConfigurationProperties(defaults, usageChecker);
        if (path.isFile()) {
            _sourceFiles.add(path);
            config.loadFile(path);
        } else if (path.isDirectory()) {
            _sourceDirectories.add(path);
            File[] files = path.listFiles();
            if (files != null) {
                Arrays.sort(files);
                for (File file : files) {
                    if (file.isFile() && file.getName().endsWith(".properties")) {
                        _sourceFiles.add(file);
                        config.loadFile(file);
                    }
                }
            }
        } else {
            /* To detect if path is created later, we consider the parent a source
             * directory. Touching that directory will cause the configuration cache
             * to be invalidated.
             */
            _sourceDirectories.add(path.getParentFile());
        }
        return config;
    }

    private ConfigurationProperties loadConfigurationByProperty(
          ConfigurationProperties defaults, String property, UsageChecker usageChecker)
          throws IOException {
        ConfigurationProperties config = defaults;
        String paths = config.getValue(property);
        if (paths != null) {
            for (String path : paths.split(PATH_DELIMITER)) {
                config = loadConfigurationFromPath(config, new File(path), usageChecker);
            }
        }
        return config;
    }

    /**
     * Loads plugins in a plugin directory.
     * <p>
     * A plugin directory contains a number of plugins. Each plugin is stored in a sub-directory
     * containing that one plugin.
     */
    private ConfigurationProperties loadPlugins(
          ConfigurationProperties defaults, File directory)
          throws IOException {
        ConfigurationProperties config = defaults;
        _sourceDirectories.add(directory);
        File[] files = directory.listFiles();
        if (files != null) {
            for (File file : files) {
                if (file.isDirectory()) {
                    config = loadConfigurationFromPath(config, file, new UniversalUsageChecker());
                }
            }
        }
        return config;
    }

    private ConfigurationProperties loadConfiguration()
          throws UnknownHostException, IOException, URISyntaxException {
        /* Configuration properties are loaded from several
         * sources, starting with importing Java system
         * properties...
         */
        ConfigurationProperties config = loadSystemProperties();
        config.setProblemConsumer(problemConsumer);

        /* ... and a chain of properties files. */
        config = loadConfigurationByProperty(config, PROPERTY_DEFAULTS_PATH,
              new UniversalUsageChecker());
        for (String dir : getPluginDirs()) {
            config = loadPlugins(config, new File(dir));
        }
        config = loadConfigurationByProperty(config, PROPERTY_SETUP_PATH,
              new DcacheConfigurationUsageChecker());

        return config;
    }

    private Layout loadLayout(ConfigurationProperties config)
          throws IOException, URISyntaxException {
        String path = config.getValue(PROPERTY_DCACHE_LAYOUT_URI);
        if (path == null) {
            throw new IOException("Undefined property: " + PROPERTY_DCACHE_LAYOUT_URI);
        }
        URI uri = new URI(path);
        Layout layout = new Layout(config);
        if (!path.isEmpty()) {
            try {
                layout.load(uri);
            } catch (FileNotFoundException e) {
                config.getProblemConsumer().warning(e.getMessage());
            }

            if (Objects.equals(uri.getScheme(), "file")) {
                _sourceFiles.add(new File(uri.getPath()));
            } else {
                layout.properties().setProperty(PROPERTY_DCACHE_CONFIG_CACHE, "false");
            }
        }
        layout.properties().setProperty(PROPERTY_DCACHE_CONFIG_FILES, joinPaths(_sourceFiles));
        layout.properties().setProperty(PROPERTY_DCACHE_CONFIG_DIRS, joinPaths(_sourceDirectories));
        return layout;
    }

    private static String joinPaths(Set<File> sourceFiles) {
        return sourceFiles.stream().map(f -> '"' + f.getPath() + '"').collect(joining(" "));
    }

    /**
     * Returns the top-level plugin directory.
     * <p>
     * To allow the plugin directory to be configurable, we first have to load all the configuration
     * files without the plugins.
     */
    private String[] getPluginDirs()
          throws IOException, URISyntaxException {
        ConfigurationProperties config = loadSystemProperties();
        config.setProblemConsumer(new SilentProblemConsumer());
        config = loadConfigurationByProperty(config, PROPERTY_DEFAULTS_PATH,
              new UniversalUsageChecker());
        config = loadConfigurationByProperty(config, PROPERTY_SETUP_PATH,
              new DcacheConfigurationUsageChecker());
        config = loadLayout(config).properties();
        String dir = config.getValue(PROPERTY_PLUGIN_PATH);
        return (dir == null) ? new String[0] : dir.split(PATH_DELIMITER);
    }

    private static class SilentProblemConsumer implements ProblemConsumer {

        @Override
        public void setFilename(String name) {
        }

        @Override
        public void setLineNumberReader(LineNumberReader reader) {
        }

        @Override
        public void error(String message) {
        }

        @Override
        public void warning(String message) {
        }

        @Override
        public void info(String message) {
        }
    }
}
