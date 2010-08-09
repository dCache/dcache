package org.dcache.boot;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.ArrayList;
import java.util.regex.Pattern;
import java.net.UnknownHostException;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;

import java.io.IOException;
import java.io.FileNotFoundException;
import java.io.File;

import dmg.util.Args;
import dmg.util.CommandException;

import org.dcache.util.ReplaceableProperties;
import org.dcache.util.DeprecatableProperties;
import org.dcache.util.Glob;

import org.slf4j.LoggerFactory;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.encoder.PatternLayoutEncoder;
import ch.qos.logback.core.ConsoleAppender;
import ch.qos.logback.classic.spi.ILoggingEvent;

/**
 * Boot loader for dCache. This class contains the main method of
 * dCache. It is responsible for parsing the configuration files and
 * for boot strapping domains.
 *
 * All methods are static. The class is never instantiated.
 */
public class BootLoader
{
    private static final String PROPERTY_DCACHE_LAYOUT_URI = "dcache.layout.uri";
    private static final String PROPERTY_HOST_NAME = "host.name";
    private static final String PROPERTY_HOST_FQDN = "host.fqdn";

    private static final String CMD_START = "start";
    private static final String CMD_LIST = "list";
    private static final String CMD_CONFIG = "config";

    private static final char OPT_SILENT = 'q';
    private static final String OPT_CONFIG_FILE = "f";
    private static final String OPT_CONFIG_FILE_DELIMITER = ":";
    private static final String OPT_DOMAIN = "domain";
    private static final String OPT_SHELL = "shell";

    private static final String CONSOLE_APPENDER_NAME = "console";
    private static final String CONSOLE_APPENDER_PATTERN = "%-5level - %msg%n";

    private BootLoader()
    {
    }

    private static void help()
    {
        System.err.println("SYNOPSIS:");
        System.err.println("  java org.dcache.util.BootLoader [-q] [-f=FILE[:FILE...]] CMD [ARGS]");
        System.err.println();
        System.err.println("OPTIONS:");
        System.err.println("    q    Do not emit warnings if configuration assigns values to deprecated");
        System.err.println("         or obsolete properties.");
        System.err.println();
        System.err.println("    f    Supply the path to the dCache setup files.  Each supplied FILE is");
        System.err.println("         is either a file or a directory.  If FILE is a file then it is read");
        System.err.println("         as a dCache configuration file.  If FILE is a directory then all");
        System.err.println("         files in that directory that end .properties are parsed.");
        System.err.println();
        System.err.println("    CMD  The operation to undertake.  The following ARGS, if required, are");
        System.err.println("         the arguments for the command.  Whether ARGS is required and their");
        System.err.println("         effect is command-specific.  The commands and possible arguments are");
        System.err.println("         described below.");
        System.err.println();
        System.err.println("COMMANDS:");
        System.err.println("   start  start the domain ARGS.  ARGS is a single word: the name of the");
        System.err.println("          domain.");
        System.err.println();
        System.err.println("   config prints all configuration values.  If -shell is included in ARGS");
        System.err.println("          then the result is formatted as assignment statements for a");
        System.err.println("          POSIX-compliant shell.  If -domain=DOMAIN is included in ARGS");
        System.err.println("          then the list will be the configuration values for domain DOMAIN");
        System.err.println();
        System.err.println("   list   list the configured domains.  If ARGS is specified then it is");
        System.err.println("          taken as a glob pattern and only those domains that match");
        System.err.println("          it will be printed.");
        System.exit(1);
    }

    private static ReplaceableProperties getDefaults()
        throws UnknownHostException
    {
        InetAddress localhost = InetAddress.getLocalHost();

        ReplaceableProperties properties =
            new ReplaceableProperties(System.getProperties());
        properties.setProperty(PROPERTY_HOST_NAME,
                               localhost.getHostName().split("\\.")[0]);
        properties.setProperty(PROPERTY_HOST_FQDN,
                               localhost.getCanonicalHostName());
        return properties;
    }

    private static ReplaceableProperties
        loadConfiguration(ReplaceableProperties config, String[] paths)
        throws IOException
    {
        for (String path: paths) {
            config = new DeprecatableProperties(config);
            File file = new File(path);
            if (file.isFile()) {
                config.loadFile(file);
            } else if (file.isDirectory()) {
                for (File entry: file.listFiles()) {
                    if (entry.isFile() && entry.getName().endsWith(".properties")) {
                        config.loadFile(entry);
                    }
                }
            }
        }
        return config;
    }

    private static Layout loadLayout(ReplaceableProperties config)
        throws IOException, URISyntaxException
    {
        Layout layout = new Layout(config);
        layout.load(new URI(config.getReplacement(PROPERTY_DCACHE_LAYOUT_URI)));
        return layout;
    }

    private static void printProperties(ReplaceableProperties properties,
                                        Set<String> keys)
    {
        for (String key: keys) {
            System.out.println(key + "=" + properties.getReplacement(key));
        }
    }

    private static void printPropertiesForShell(ReplaceableProperties properties,
                                                Set<String> keys)
    {
        for (String key: keys) {
            String var = key.toUpperCase().replace('.', '_').replace('-', '_');
            String value = properties.getReplacement(key).replace("\\", "\\\\").replace("$", "\\$").replace("`", "\\`").replace("\"", "\\\"");
            System.out.println(var + "=\"" + value + "\"");
        }
    }

    private static Collection<Pattern> toPatterns(Args args)
    {
        List<Pattern> patterns = new ArrayList<Pattern>();
        for (int i = 0; i < args.argc(); i++) {
            patterns.add(new Glob(args.argv(i)).toPattern());
        }
        return patterns;
    }

    public static void main(String arguments[])
    {
        try {
            Args args = new Args(arguments);
            if (args.argc() < 1) {
                help();
            }

            /* Basic logging setup that will be used until the real
             * log4j configuration is loaded.
             */
            Level level = args.isOneCharOption( OPT_SILENT) ? Level.ERROR : Level.WARN;
            logToConsoleAtLevel( level);

            ReplaceableProperties config = getDefaults();
            String tmp = args.getOpt(OPT_CONFIG_FILE);
            if (tmp != null) {
                config =
                    loadConfiguration(config, tmp.split(OPT_CONFIG_FILE_DELIMITER));
            }

            Layout layout = loadLayout(config);
            String command = args.argv(0);
            if (command.equals(CMD_START)) {
                if (args.argc() != 2) {
                    throw new IllegalArgumentException("Missing argument: Domain name");
                }
                Domain domain = layout.getDomain(args.argv(1));
                if (domain == null) {
                    throw new IllegalArgumentException("No such domain: " + args.argv(1));
                }

                domain.start();
            } else if (command.equals(CMD_LIST)) {
                args.shift();
                if (args.argc() == 0) {
                    layout.printDomainNames(System.out);
                } else {
                    layout.printMatchingDomainNames(System.out, toPatterns(args));
                }
            } else if (command.equals(CMD_CONFIG)) {
                ReplaceableProperties domainConfig;
                if (args.getOpt(OPT_DOMAIN) != null) {
                    Domain domain = layout.getDomain(args.getOpt(OPT_DOMAIN));
                    if (domain == null) {
                        throw new IllegalArgumentException("No such domain: " + args.getOpt(OPT_DOMAIN));
                    }
                    domainConfig = domain.properties();
                } else {
                    domainConfig = layout.properties();
                }

                args.shift();
                Set<String> keys =
                    (args.argc() == 0)
                    ? domainConfig.stringPropertyNames()
                    : domainConfig.matchingStringPropertyNames(toPatterns(args));

                if (args.getOpt(OPT_SHELL) != null) {
                    printPropertiesForShell(domainConfig, keys);
                } else {
                    printProperties(domainConfig, keys);
                }
            } else {
                throw new IllegalArgumentException("Invalid command: " + command);
            }
        } catch (IllegalArgumentException e) {
            System.err.println(e.getMessage());
            System.exit(1);
        } catch (FileNotFoundException e) {
            System.err.println(e.getMessage());
            System.exit(1);
        } catch (IOException e) {
            System.err.println(e.toString());
            System.exit(1);
        } catch (URISyntaxException e) {
            System.err.println(e.getMessage());
            System.exit(1);
        } catch (CommandException e) {
            System.err.println(e.getMessage());
            System.exit(1);
        } catch (RuntimeException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    private static void logToConsoleAtLevel(Level level)
    {
        LoggerContext loggerContext =
            (LoggerContext) LoggerFactory.getILoggerFactory();
        loggerContext.reset();

        ConsoleAppender<ILoggingEvent> ca =
            new ConsoleAppender<ILoggingEvent>();
        ca.setContext(loggerContext);
        ca.setName(CONSOLE_APPENDER_NAME);
        PatternLayoutEncoder pl = new PatternLayoutEncoder();
        pl.setContext(loggerContext);
        pl.setPattern(CONSOLE_APPENDER_PATTERN);
        pl.start();

        ca.setEncoder(pl);
        ca.start();
        Logger rootLogger = loggerContext.getLogger(Logger.ROOT_LOGGER_NAME);
        rootLogger.addAppender(ca);
        rootLogger.setLevel(level);
    }
}