package org.dcache.boot;

import java.util.logging.LogManager;
import java.net.UnknownHostException;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;

import java.io.IOException;
import java.io.FileNotFoundException;
import java.io.File;

import dmg.util.Args;
import dmg.util.CommandException;

import org.dcache.util.ConfigurationProperties;

import org.slf4j.bridge.SLF4JBridgeHandler;
import org.slf4j.LoggerFactory;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.encoder.PatternLayoutEncoder;
import ch.qos.logback.core.ConsoleAppender;
import ch.qos.logback.classic.spi.ILoggingEvent;

import static org.dcache.boot.Properties.*;

/**
 * Boot loader for dCache. This class contains the main method of
 * dCache. It is responsible for parsing the configuration files and
 * for boot strapping domains.
 *
 * All methods are static. The class is never instantiated.
 */
public class BootLoader
{
    private static final String CMD_START = "start";
    private static final String CMD_COMPILE = "compile";
    private static final String CMD_COMPILE_OP_SHELL = "shell";
    private static final String CMD_COMPILE_OP_XML = "xml";

    private static final char OPT_SILENT = 'q';
    private static final String OPT_CONFIG_FILE = "f";
    private static final String OPT_CONFIG_FILE_DELIMITER = ":";

    private static final String CONSOLE_APPENDER_NAME = "console";
    private static final String CONSOLE_APPENDER_PATTERN = "%-5level - %msg%n";

    private BootLoader()
    {
    }

    private static void help()
    {
        System.err.println("SYNOPSIS:");
        System.err.println("  java org.dcache.util.BootLoader [-q] [-f=PATH[:PATH...]] COMMAND [ARGS]");
        System.err.println();
        System.err.println("OPTIONS:");
        System.err.println("    -q    Suppress warnings.");
        System.err.println("    -f    Paths to the dCache setup files.");
        System.err.println();
        System.err.println("COMMANDS:");
        System.err.println("   start DOMAIN");
        System.err.println("          Start a domain.");
        System.err.println();
        System.err.println("   " + CMD_COMPILE + " <format>");
        System.err.println("          Compiles the layout to some particular format, determined by <format>.");
        System.err.println("          Valid values of <format> are:");
        System.err.println("                  -" + CMD_COMPILE_OP_SHELL + " POSIX shell declaration of an oracle function");
        System.err.println("                  -" + CMD_COMPILE_OP_XML + " an set of XML entity definitions");
        System.exit(1);
    }

    private static ConfigurationProperties getDefaults()
        throws UnknownHostException
    {
        InetAddress localhost = InetAddress.getLocalHost();

        ConfigurationProperties properties =
            new ConfigurationProperties(System.getProperties());
        properties.setProperty(PROPERTY_HOST_NAME,
                               localhost.getHostName().split("\\.")[0]);
        properties.setProperty(PROPERTY_HOST_FQDN,
                               localhost.getCanonicalHostName());
        return properties;
    }

    private static ConfigurationProperties
        loadConfiguration(ConfigurationProperties config, String[] paths)
        throws IOException
    {
        for (String path: paths) {
            config = new ConfigurationProperties(config);
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

    private static Layout loadLayout(ConfigurationProperties config)
        throws IOException, URISyntaxException
    {
        Layout layout = new Layout(config);
        layout.load(new URI(config.getValue(PROPERTY_DCACHE_LAYOUT_URI)));
        return layout;
    }

    public static void main(String arguments[])
    {
        try {
            Args args = new Args(arguments);
            if (args.argc() < 1) {
                help();
            }

            /* Redirects Java util logging to SLF4J.
             */
            LogManager.getLogManager().reset();
            SLF4JBridgeHandler.install();

            /* Basic logging setup that will be used until the real
             * log configuration is loaded.
             */
            Level level =
                args.isOneCharOption(OPT_SILENT) ? Level.ERROR : Level.WARN;
            logToConsoleAtLevel(level);

            ConfigurationProperties config = getDefaults();
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
            } else if (command.equals(CMD_COMPILE)) {
                LayoutPrinter printer = printerForArgs(args, layout);
                printer.print(System.out);
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

    private static LayoutPrinter printerForArgs(Args args, Layout layout)
    {
        boolean compileForShell = args.getOption(CMD_COMPILE_OP_SHELL) != null;
        boolean compileForXml = args.getOption(CMD_COMPILE_OP_XML) != null;

        if(compileForShell == compileForXml) {
            throw new IllegalArgumentException("Must specify exactly one of -" +
                    CMD_COMPILE_OP_SHELL + " and -" + CMD_COMPILE_OP_XML);
        }

        return compileForShell ? new ShellOracleLayoutPrinter(layout)
                    : new XmlEntityLayoutPrinter(layout);
    }

    private static void logToConsoleAtLevel(Level level)
    {
        LoggerContext loggerContext =
            (LoggerContext) LoggerFactory.getILoggerFactory();
        loggerContext.reset();

        ConsoleAppender<ILoggingEvent> ca =
            new ConsoleAppender<ILoggingEvent>();
        ca.setTarget("System.err");
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