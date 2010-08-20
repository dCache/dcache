package org.dcache.boot;

import java.util.Collection;
import java.util.Collections;
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
import java.io.PrintStream;

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
    private static final String CMD_COMPILE = "compile";

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
        System.err.println("   compile");
        System.err.println("          Compiles the layout to a shell script.");
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

    private static String quote(String s)
    {
        return s.replace("\\", "\\\\").replace("$", "\\$").replace("`", "\\`").replace("\"", "\\\"");
    }

    private static String quoteForCase(String s)
    {
        return quote(s).replace(")", "\\)").replace("?", "\\?").replace("*", "\\*").replace("[", "\\[");
    }

    private static void compileToShell(ReplaceableProperties properties)
    {
        PrintStream out = System.out;
        out.println("      case \"$1\" in");
        for (String key: properties.stringPropertyNames()) {
            out.println("        " + quoteForCase(key) + ") echo \"" + quote(properties.getReplacement(key)) + "\";;");
        }
        out.println("        *) undefinedProperty \"$1\" \"$2\";;");
        out.println("      esac");
    }

    private static void compileToShell(Layout layout)
    {
        PrintStream out = System.out;
        out.println("getProperty()");
        out.println("{");
        out.println("  case \"$2\" in");
        out.println("    \"\")");
        compileToShell(layout.properties());
        out.println("      ;;");
        for (Domain domain: layout.getDomains()) {
            out.println("    " + quoteForCase(domain.getName()) + ")");
            compileToShell(domain.properties());
            out.println("      ;;");
        }
        out.println("    *)");
        out.println("      undefinedDomain \"$1\"");
        out.println("      ;;");
        out.println("  esac;");
        out.println("}");
    }

    public static void main(String arguments[])
    {
        try {
            Args args = new Args(arguments);
            if (args.argc() < 1) {
                help();
            }

            /* Basic logging setup that will be used until the real
             * log configuration is loaded.
             */
            Level level =
                args.isOneCharOption(OPT_SILENT) ? Level.ERROR : Level.WARN;
            logToConsoleAtLevel(level);

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
            } else if (command.equals(CMD_COMPILE)) {
                compileToShell(layout);
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