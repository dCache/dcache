package org.dcache.boot;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import ch.qos.logback.core.joran.spi.JoranException;
import ch.qos.logback.core.util.StatusPrinter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import dmg.cells.nucleus.CDC;
import dmg.cells.nucleus.CellShell;
import dmg.cells.nucleus.SystemCell;
import dmg.util.Args;
import dmg.util.CommandException;

import org.dcache.util.ConfigurationProperties;
import org.dcache.util.NetworkUtils;

import static org.dcache.boot.Properties.*;

/**
 * Domain encapsulates the configuration of a domain and its
 * services. Provides the logic for starting a domain.
 */
public class Domain
{
    private static final String SYSTEM_CELL_NAME = "System";
    private static final String SCHEME_FILE = "file";

    private static final Logger _log =
        LoggerFactory.getLogger(SystemCell.class);

    private final ConfigurationProperties _properties;
    private final List<ConfigurationProperties> _services;

    public Domain(String name, ConfigurationProperties defaults)
    {
        _properties = new ConfigurationProperties(defaults);
        _properties.put(PROPERTY_DOMAIN_NAME, name);
        _services = new ArrayList<>();
    }

    public ConfigurationProperties properties()
    {
        return _properties;
    }

    public List<String> getCellNames()
    {
        List<String> cells = new ArrayList<>();
        for (ConfigurationProperties service: _services) {
            String cellName = Properties.getCellName(service);
            if (cellName != null) {
                cells.add(cellName);
            }
        }
        return cells;
    }

    public ConfigurationProperties createService(String type)
    {
        ConfigurationProperties service = new ConfigurationProperties(_properties);
        service.setIsService(true);
        service.put(PROPERTY_DOMAIN_SERVICE, type);
        _services.add(service);
        return service;
    }

    public String getName()
    {
        return _properties.getValue(PROPERTY_DOMAIN_NAME);
    }

    List<ConfigurationProperties> getServices()
    {
        return _services;
    }

    public void start()
        throws URISyntaxException, CommandException, IOException
    {
        initializeLogging();

        String domainName = getName();
        CDC.reset(SYSTEM_CELL_NAME, domainName);
        SystemCell systemCell = new SystemCell(domainName);
        _log.info("Starting " + domainName);

        executePreload(systemCell);
        for (ConfigurationProperties serviceConfig: _services) {
            executeService(systemCell, serviceConfig);
        }

        if (_services.isEmpty()) {
            _log.warn("No services found. Domain appears to be empty.");
        }
    }

    private URI getLogConfigurationUri()
        throws URISyntaxException
    {
        String property = _properties.getValue(PROPERTY_LOG_CONFIG);
        if (property == null) {
            return null;
        }

        URI uri = new URI(property);
        String path = uri.getPath();
        if (path == null) {
            throw new URISyntaxException(property, "Path is missing");
        }

        if (uri.getScheme() == null || uri.getScheme().equals(SCHEME_FILE)) {
            File f = new File(path);
            uri = f.toURI();
        }
        return uri;
    }

    private void initializeLogging()
        throws URISyntaxException, IOException
    {
        try {
            URI uri = getLogConfigurationUri();
            if (uri == null) {
                return;
            }

            LoggerContext loggerContext =
                (LoggerContext) LoggerFactory.getILoggerFactory();
            loggerContext.reset();

            for (String key: _properties.stringPropertyNames()) {
                loggerContext.putProperty(key, _properties.getProperty(key));
            }

            try {
                JoranConfigurator configurator = new JoranConfigurator();
                configurator.setContext(loggerContext);
                configurator.doConfigure(NetworkUtils.toURL(uri));
            } finally {
                StatusPrinter.printInCaseOfErrorsOrWarnings(loggerContext);
            }
        } catch (JoranException e) {
            throw new IOException("Failed to load log configuration:" + e.getMessage(), e);
        }
    }

    /**
     * Imports all properties.
     */
    private void importParameters(Map<String,Object> map,
                                  ConfigurationProperties properties)
    {
        for (String key: properties.stringPropertyNames()) {
            map.put(key, properties.getProperty(key));
        }
    }

    /**
     * Executes a preload batch script, if defined.
     */
    private void executePreload(SystemCell cell)
        throws URISyntaxException, IOException, CommandException
    {
        String preload = _properties.getValue(PROPERTY_DOMAIN_PRELOAD);
        if (preload != null) {
            CellShell shell = new CellShell(cell.getNucleus());
            importParameters(shell.environment(), _properties);
            executeBatchFile(shell, new URI(preload));
        }
    }

    /**
     * Creates a CellShell preloaded with a specific service's
     * configuration.
     */
    CellShell createShellForService(SystemCell system, ConfigurationProperties properties)
    {
        CellShell shell = new CellShell(system.getNucleus());
        importParameters(shell.environment(), properties);
        return shell;
    }

    /**
     * Executes the batch file of the service.
     */
    private void executeService(SystemCell system, ConfigurationProperties properties)
        throws URISyntaxException, IOException, CommandException
    {
        CellShell shell = createShellForService(system, properties);
        URI uri = findBatchFile(properties);
        executeBatchFile(shell, uri);
    }

    /**
     * Scans the service directories to locate the service batch file.
     */
    private URI findBatchFile(ConfigurationProperties properties)
        throws URISyntaxException
    {
        String name = properties.getValue(PROPERTY_DOMAIN_SERVICE_URI);

        /* Don't search if we have an absolute position.
         */
        URI uri = new URI(name);
        if (uri.isAbsolute()) {
            return uri;
        }

        File file = new File(uri.getPath());
        if (file.isAbsolute()) {
            return file.toURI();
        }

        /* Search in plugin directories.
         */
        String pluginPath = properties.getValue(PROPERTY_PLUGIN_PATH);
        for (String s: pluginPath.split(PATH_DELIMITER)) {
            File dir = new File(s);
            if (dir.isDirectory()) {
                for (File plugin: dir.listFiles()) {
                    file = new File(plugin, name);
                    if (file.exists()) {
                        return file.toURI();
                    }
                }
            }
        }

        /* Resolve relativ to base path.
         */
        URI base =
            new URI(properties.getValue(PROPERTY_DOMAIN_SERVICE_URI_BASE));
        return base.resolve(uri);
    }

    /**
     * Executes the batch file in the resource.
     */
    private void executeBatchFile(CellShell shell, URI resource)
        throws URISyntaxException, IOException, CommandException
    {
        try (InputStream input = NetworkUtils.toURL(resource).openStream()) {
            shell.execute(resource.toString(), new InputStreamReader(input),
                    new Args(""));
        }

    }
}
