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
import org.dcache.util.Glob;

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

    private static final String OPT_CONFIG_FILE = "f";
    private static final String OPT_CONFIG_FILE_DELIMITER = ":";
    private static final String OPT_DOMAIN = "domain";
    private static final String OPT_SHELL = "shell";

    private BootLoader()
    {
    }

    private static void help()
    {
        System.err.println("Usage:");
        System.err.println();
        System.err.println("  java org.dcache.util.BootLoader [-f=FILE[:FILE...]] CMD [ARGS]");
        System.err.println();
        System.err.println("where FILE is a path to a dCache setup file and");
        System.err.println("CMD [ARGS] is one of the following commands:");
        System.err.println();
        System.err.println(" start DOMAIN");
        System.err.println("      starts domain DOMAIN");
        System.err.println(" config [-shell] [KEY ...]");
        System.err.println("      prints configuration values");
        System.err.println(" config [-shell] -domain=DOMAIN [KEY ...]");
        System.err.println("      prints domain specific configuration values");
        System.err.println(" list");
        System.err.println("      lists all configured domains");
        System.err.println(" list PATTERN");
        System.err.println("      lists all configured domains matching a given pattern");
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
            config = new ReplaceableProperties(config);
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
            String var = key.toUpperCase().replace('.', '_').replace('-', '-').replace("\\", "\\\\").replace("$", "\\$").replace("\"", "\\\"");
            String value = properties.getReplacement(key);
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
}