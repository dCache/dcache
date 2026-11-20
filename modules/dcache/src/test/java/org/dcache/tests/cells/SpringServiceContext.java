/*
 * dCache - http://www.dcache.org/
 *
 * Copyright (C) 2026 Deutsches Elektronen-Synchrotron
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.dcache.tests.cells;

import static java.util.stream.Collectors.joining;

import dmg.cells.nucleus.CellAddressCore;
import dmg.cells.nucleus.CellArgsAware;
import dmg.cells.nucleus.CellCommandListener;
import dmg.cells.nucleus.CellEndpoint;
import dmg.cells.nucleus.CellIdentityAware;
import dmg.cells.nucleus.CellInfo;
import dmg.cells.nucleus.CellInfoAware;
import dmg.cells.nucleus.CellLifeCycleAware;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellMessageAnswerable;
import dmg.cells.nucleus.CellMessageSender;
import dmg.cells.nucleus.DomainContextAware;
import dmg.cells.nucleus.EnvironmentAware;
import dmg.cells.nucleus.NoRouteToCellException;
import dmg.util.CommandException;
import dmg.util.CommandInterpreter;
import java.io.File;
import java.io.IOException;
import java.io.LineNumberReader;
import java.io.Reader;
import java.io.Serializable;
import java.io.StringReader;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.TestingServer;
import org.dcache.boot.Domain;
import org.dcache.cells.CuratorFrameworkAware;
import org.dcache.cells.ThreadCreator;
import org.dcache.cells.ZoneAware;
import org.dcache.util.Args;
import org.dcache.util.configuration.ConfigurationProperties;
import org.dcache.util.spring.RequiredPropertyChecker;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.context.support.GenericXmlApplicationContext;
import org.springframework.core.env.MapPropertySource;

/**
 * Starts the Spring context of a dCache service outside a cell, as a smoke test.
 * <p>
 * The properties are those a domain would pass to the service: the defaults shipped in
 * {@code skel/share/defaults} (filtered as for the tarball), resolved for the service type, plus
 * the given overrides. Beans get what {@code UniversalSpringCell} injects; the cell endpoint
 * behaves as if no other cell existed, so requests fail with {@code NoRouteToCellException}.
 * ZooKeeper is an in-process test server. {@link RequiredPropertyChecker} is active and
 * {@link CellLifeCycleAware} beans are started and stopped as in a cell.
 */
public class SpringServiceContext implements AutoCloseable {

    private final TestingServer zookeeper;
    private final CuratorFramework curator;
    private final GenericXmlApplicationContext context;
    private final List<CellLifeCycleAware> lifeCycleAware = new ArrayList<>();
    private final Map<String, Object> domainContext = new ConcurrentHashMap<>();
    private final CommandInterpreter commands = new CommandInterpreter();

    private SpringServiceContext(String serviceType, String xml, Map<String, String> overrides,
          String... profiles) throws Exception {
        zookeeper = new TestingServer();
        curator = CuratorFrameworkFactory.newClient(zookeeper.getConnectString(),
              new RetryOneTime(100));
        curator.start();
        context = new GenericXmlApplicationContext();
        try {
            Map<String, Object> environment = serviceProperties(serviceType, overrides);
            RequiredPropertyChecker.register(context);
            context.addBeanFactoryPostProcessor(
                  factory -> factory.addBeanPostProcessor(new CellInjector(environment)));
            context.getEnvironment().getPropertySources().addFirst(
                  new MapPropertySource(serviceType, environment));
            context.getEnvironment().setActiveProfiles(profiles);
            context.load(xml);
            context.refresh();
            context.getBeansOfType(CellCommandListener.class).values()
                  .forEach(commands::addCommandListener);
            lifeCycleAware.addAll(context.getBeansOfType(CellLifeCycleAware.class).values());
            lifeCycleAware.forEach(CellLifeCycleAware::afterStart);
        } catch (Exception e) {
            try {
                close();
            } catch (Exception suppressed) {
                e.addSuppressed(suppressed);
            }
            throw e;
        }
    }

    /**
     * Starts the context.
     *
     * @param serviceType the service type as in a layout file, e.g. {@code webdav}
     * @param xml         location of the Spring XML, e.g. {@code classpath:org/dcache/webdav/webdav.xml}
     * @param overrides   properties set in the layout for this service
     * @param profiles    Spring profiles as given in the service's batch file
     */
    public static SpringServiceContext start(String serviceType, String xml,
          Map<String, String> overrides, String... profiles) throws Exception {
        return new SpringServiceContext(serviceType, xml, overrides, profiles);
    }

    public <T> T getBean(String name, Class<T> type) {
        return context.getBean(name, type);
    }

    /**
     * Returns the value of a property of the service, with placeholders resolved.
     */
    public String property(String name) {
        return context.getEnvironment().getRequiredProperty(name);
    }

    /**
     * Returns the domain context, as shared by the cells of a domain.
     */
    public Map<String, Object> domainContext() {
        return domainContext;
    }

    /**
     * Executes an admin command of the service, as in its batch file or the admin shell.
     */
    public Serializable command(String line) throws CommandException {
        return commands.command(new Args(line));
    }

    /**
     * Returns the local port of the first connector of the Jetty server in bean {@code jetty}.
     */
    public int jettyPort() {
        Server jetty = getBean("jetty", Server.class);
        return ((ServerConnector) jetty.getConnectors()[0]).getLocalPort();
    }

    @Override
    public void close() throws IOException {
        try {
            lifeCycleAware.forEach(CellLifeCycleAware::beforeStop);
            context.close();
        } finally {
            curator.close();
            zookeeper.close();
        }
    }

    /**
     * Injects what UniversalSpringCell injects.
     */
    private class CellInjector implements BeanPostProcessor {

        private final Map<String, Object> environment;

        CellInjector(Map<String, Object> environment) {
            this.environment = environment;
        }

        @Override
        public Object postProcessBeforeInitialization(Object bean, String beanName) {
            if (bean instanceof EnvironmentAware aware) {
                aware.setEnvironment(environment);
            }
            if (bean instanceof CellArgsAware aware) {
                aware.setCellArgs(new Args(""));
            }
            if (bean instanceof DomainContextAware aware) {
                aware.setDomainContext(domainContext);
            }
            if (bean instanceof CuratorFrameworkAware aware) {
                aware.setCuratorFramework(curator);
            }
            if (bean instanceof CellIdentityAware aware) {
                aware.setCellAddress(new CellAddressCore("test", "testDomain"));
            }
            if (bean instanceof CellInfoAware aware) {
                aware.setCellInfoSupplier(CellInfo::new);
            }
            if (bean instanceof ZoneAware aware) {
                aware.setZone(Optional.empty());
            }
            if (bean instanceof ThreadCreator aware) {
                aware.setThreadFactory(Executors.defaultThreadFactory());
            }
            if (bean instanceof CellMessageSender sender) {
                sender.setCellEndpoint(new NoCellsEndpoint());
            }
            return bean;
        }
    }

    /**
     * The endpoint of a cell alone in its domain: requests fail with NoRouteToCellException, other
     * messages are dropped.
     */
    private class NoCellsEndpoint implements CellEndpoint {

        @Override
        public void sendMessage(CellMessage envelope, SendFlag... flags) {
        }

        @Override
        public void sendMessage(CellMessage envelope, CellMessageAnswerable callback,
              Executor executor, long timeout, SendFlag... flags) {
            executor.execute(() -> callback.exceptionArrived(envelope,
                  new NoRouteToCellException(envelope, "No other cells in this test")));
        }

        @Override
        public Map<String, Object> getDomainContext() {
            return domainContext;
        }
    }

    private static Map<String, Object> serviceProperties(String serviceType,
          Map<String, String> overrides) throws IOException {
        File root = findSourceRoot();
        Properties filter = new Properties();
        try (Reader in = Files.newBufferedReader(
              root.toPath().resolve("packages/tar/src/main/assembly/filter.properties"))) {
            filter.load(in);
        }

        /* The defaults are filtered when packaged; do the same as the tarball. */
        ConfigurationProperties defaults = new ConfigurationProperties(new Properties());
        File[] files = new File(root, "skel/share/defaults").listFiles(
              f -> f.isFile() && f.getName().endsWith(".properties"));
        Arrays.sort(files);
        for (File file : files) {
            String text = Files.readString(file.toPath());
            for (String token : filter.stringPropertyNames()) {
                text = text.replace("@" + token + "@", filter.getProperty(token));
            }
            defaults.load(file.getName(), new LineNumberReader(new StringReader(text)));
        }

        /* Values a dCache installation sets at boot time. */
        Map<String, String> installation = new LinkedHashMap<>();
        installation.put("host.name", "localhost");
        installation.put("host.fqdn", "localhost");
        installation.put("dcache.version", "test");
        installation.put("dcache.scm-state", "test");
        installation.put("dcache.home", new File(root, "skel").getPath());
        installation.put("dcache.paths.plugins", "");

        ConfigurationProperties layout = new ConfigurationProperties(defaults);
        layout.load(new StringReader(asPropertiesText(installation)));
        Domain domain = new Domain("testDomain", layout);
        ConfigurationProperties service = domain.createService("test", new LineNumberReader(
              new StringReader(asPropertiesText(overrides))), serviceType);

        Map<String, Object> properties = new HashMap<>();
        for (String key : service.stringPropertyNames()) {
            properties.put(key, service.getProperty(key));
        }
        return properties;
    }

    private static String asPropertiesText(Map<String, String> properties) {
        return properties.entrySet().stream()
              .map(e -> e.getKey() + "=" + e.getValue().replace("\\", "\\\\") + "\n")
              .collect(joining());
    }

    /**
     * Returns the root of the source tree, searched upwards from the working directory (the module
     * directory when run by Maven).
     */
    private static File findSourceRoot() {
        for (File dir = new File("").getAbsoluteFile(); dir != null; dir = dir.getParentFile()) {
            if (new File(dir, "skel/share/defaults").isDirectory()) {
                return dir;
            }
        }
        throw new IllegalStateException("skel/share/defaults not found above "
              + new File("").getAbsolutePath());
    }
}
