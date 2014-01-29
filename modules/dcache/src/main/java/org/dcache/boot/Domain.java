package org.dcache.boot;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import ch.qos.logback.core.joran.spi.JoranException;
import ch.qos.logback.core.util.StatusPrinter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.FileSystemResourceLoader;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import dmg.cells.nucleus.CDC;
import dmg.cells.nucleus.CellShell;
import dmg.cells.nucleus.SystemCell;
import dmg.util.CommandException;

import org.dcache.util.Args;
import org.dcache.util.ConfigurationProperties;

import static org.dcache.boot.Properties.*;

/**
 * Domain encapsulates the configuration of a domain and its
 * services. Provides the logic for starting a domain.
 */
public class Domain
{
    private static final String SYSTEM_CELL_NAME = "System";

    private static final Logger _log =
        LoggerFactory.getLogger(SystemCell.class);

    private final ConfigurationProperties _properties;
    private final List<ConfigurationProperties> _services;
    private final ResourceLoader _resourceLoader = new FileSystemResourceLoader();

    public Domain(String name, ConfigurationProperties defaults)
    {
        _properties = new ConfigurationProperties(defaults, new DcacheConfigurationUsageChecker());
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

    public ConfigurationProperties createService(String source, LineNumberReader reader, String type) throws IOException
    {
        ConfigurationProperties service = new ConfigurationProperties(_properties, new DcacheConfigurationUsageChecker());
        service.setIsService(true);
        service.put(PROPERTY_DOMAIN_SERVICE, type);
        service.load(source, reader);

        try {
            Resource batchFile = findBatchFile(service);
            service.put(PROPERTY_DOMAIN_SERVICE_BATCH, batchFile.getURI().toString());
            checkExists(batchFile);

            Resource logConfiguration = getLogConfiguration();
            if (logConfiguration != null) {
                checkExists(logConfiguration);
            }
        } catch (IOException e) {
            service.getProblemConsumer().setFilename(source);
            service.getProblemConsumer().setLineNumberReader(reader);
            service.getProblemConsumer().error(e.getMessage());
        }

        _services.add(service);
        return service;
    }

    private void checkExists(Resource resource) throws IOException
    {
        if (!resource.exists()) {
            throw new IOException(resource.getDescription() + " (no such resource)");
        }
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

    private Resource getLogConfiguration()
    {
        String property = _properties.getValue(PROPERTY_LOG_CONFIG);
        return (property == null) ? null : _resourceLoader.getResource(property);
    }

    private void initializeLogging()
        throws URISyntaxException, IOException
    {
        try {
            Resource configuration = getLogConfiguration();
            if (configuration == null) {
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
                configurator.doConfigure(configuration.getURL());
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
            executeBatchFile(shell, _resourceLoader.getResource(preload));
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
        Resource resource = _resourceLoader.getResource(properties.getValue(PROPERTY_DOMAIN_SERVICE_BATCH));
        executeBatchFile(shell, resource);
    }

    /**
     * Scans the service directories to locate the service batch file.
     */
    private Resource findBatchFile(ConfigurationProperties properties)
            throws IOException
    {
        String name = properties.getValue(PROPERTY_DOMAIN_SERVICE_URI);
        if (name == null) {
            throw new IOException("Property " + PROPERTY_DOMAIN_SERVICE_URI + " is undefined");
        }

        /* Don't search if the URI is absolute.
         */
        URI uri;
        try {
            uri = new URI(name);
        } catch (URISyntaxException e) {
            throw new IOException(e.getMessage(), e);
        }
        if (uri.isAbsolute()) {
            return _resourceLoader.getResource(name);
        }

        /* Search in plugin directories.
         */
        String pluginPath = properties.getValue(PROPERTY_PLUGIN_PATH);
        for (String dir: pluginPath.split(PATH_DELIMITER)) {
            File[] files = new File(dir).listFiles();
            if (files != null) {
                for (File plugin: files) {
                    File file = new File(plugin, name);
                    if (file.exists()) {
                        return new FileSystemResource(file);
                    }
                }
            }
        }

        /* Resolve relative to base URI.
         */
        Resource base = _resourceLoader.getResource(properties.getValue(PROPERTY_DOMAIN_SERVICE_URI_BASE));
        return base.createRelative(name);
    }

    /**
     * Executes the batch file in the resource.
     */
    private void executeBatchFile(CellShell shell, Resource resource)
        throws URISyntaxException, IOException, CommandException
    {
        try (InputStream input = resource.getInputStream()) {
            shell.execute(resource.toString(), new InputStreamReader(input),
                    new Args(""));
        }
    }

}
