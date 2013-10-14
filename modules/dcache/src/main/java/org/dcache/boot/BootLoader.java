package org.dcache.boot;

import org.slf4j.LoggerFactory;
import org.slf4j.bridge.SLF4JBridgeHandler;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.LineNumberReader;
import java.io.OutputStream;
import java.io.PrintStream;
import java.net.URISyntaxException;
import java.util.logging.LogManager;

import dmg.util.Args;
import dmg.util.CommandException;

import org.dcache.util.ConfigurationProperties.DefaultProblemConsumer;
import org.dcache.util.ConfigurationProperties.ProblemConsumer;

import static com.google.common.base.Throwables.getRootCause;

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
    private static final String CMD_COMPILE_OP_PYTHON = "python";
    private static final String CMD_COMPILE_OP_XML = "xml";
    private static final String CMD_COMPILE_OP_DEBUG = "debug";
    private static final String CMD_CHECK = "check-config";

    private static final char OPT_SILENT = 'q';

    private BootLoader()
    {
    }

    private static void help()
    {
        System.err.println("SYNOPSIS:");
        System.err.println("  java org.dcache.util.BootLoader [-q] COMMAND [ARGS]");
        System.err.println();
        System.err.println("OPTIONS:");
        System.err.println("    -q    Suppress warnings and errors.");
        System.err.println();
        System.err.println("COMMANDS:");
        System.err.println("   start DOMAIN");
        System.err.println("          Start a domain.");
        System.err.println();
        System.err.println("   " + CMD_CHECK);
        System.err.println("          Check configuration for any problems.");
        System.err.println();
        System.err.println("   " + CMD_COMPILE + " <format>");
        System.err.println("          Compiles the layout to some particular format, determined by <format>.");
        System.err.println("          Valid values of <format> are:");
        System.err.println("                  -" + CMD_COMPILE_OP_SHELL + " POSIX shell declaration of an oracle function");
        System.err.println("                  -" + CMD_COMPILE_OP_PYTHON + " Python declaration of an oracle function");
        System.err.println("                  -" + CMD_COMPILE_OP_XML + " an set of XML entity definitions");
        System.err.println("                  -" + CMD_COMPILE_OP_DEBUG + " a format providing human-readable format");
        System.exit(1);
    }


    public static void main(String arguments[])
    {
        String command = null;

        try {
            Args args = new Args(arguments);
            if (args.argc() < 1) {
                help();
            }
            command = args.argv(0);

            /* Redirects Java util logging to SLF4J.
             */
            LogManager.getLogManager().reset();
            SLF4JBridgeHandler.install();

            /* Configuration problems can be directed to the log
             * system or to stdout, depending on which command was
             * provided on the command line.
             */
            OutputStreamProblemConsumer checkConsumer =
                new OutputStreamProblemConsumer(System.out);
            ProblemConsumer problemConsumer =
                command.equals(CMD_CHECK)
                ? checkConsumer
                : new DefaultProblemConsumer();
            problemConsumer =
                args.isOneCharOption(OPT_SILENT)
                ? new ErrorsAsWarningsProblemConsumer(problemConsumer)
                : problemConsumer;

            /* The layout contains all configuration information, and
             * all domains and services of this node.
             */
            Layout layout = new LayoutBuilder().setProblemConsumer(problemConsumer).build();

            /* The BootLoader is not limited to starting dCache. The
             * behaviour is controlled by a command provided as a
             * command line argument.
             */
            switch (command) {
            case CMD_START:
                if (args.argc() != 2) {
                    throw new IllegalArgumentException("Missing argument: Domain name");
                }
                Domain domain = layout.getDomain(args.argv(1));
                if (domain == null) {
                    throw new IllegalArgumentException("No such domain: " + args
                            .argv(1));
                }

                domain.start();
                break;
            case CMD_CHECK:
                checkConsumer.printSummary();
                System.exit(checkConsumer.getReturnCode());
            case CMD_COMPILE:
                LayoutPrinter printer = printerForArgs(args, layout);
                printer.print(System.out);
                break;
            default:
                throw new IllegalArgumentException("Invalid command: " + command);
            }
        } catch (IllegalArgumentException | CommandException | URISyntaxException | FileNotFoundException e) {
            handleFatalError(e.getMessage(), command);
        } catch (IOException e) {
            handleFatalError(e.toString(), command);
        } catch (RuntimeException e) {
            handleFatalError(e, command);
        }
    }

    private static void handleFatalError(Object message, String command) {
        if (CMD_START.equals(command)) {
            String logMessage;

            if (message instanceof RuntimeException) {
                logMessage = "Bug found, please report the following " +
                        "information to support@dcache.org";
            } else {
                logMessage = "Failure at startup: {}";
            }

            LoggerFactory.getLogger("root").error(logMessage, message);
        } else {
            if (message instanceof RuntimeException) {
                getRootCause((RuntimeException)message).printStackTrace();
            } else {
                System.err.println(message);
            }
        }

        System.exit(1);
    }

    private static LayoutPrinter printerForArgs(Args args, Layout layout)
    {
        boolean compileForShell = args.hasOption(CMD_COMPILE_OP_SHELL);
        boolean compileForXml = args.hasOption(CMD_COMPILE_OP_XML);
        boolean compileForDebug = args.hasOption(CMD_COMPILE_OP_DEBUG);
        boolean compileForPython = args.hasOption(CMD_COMPILE_OP_PYTHON);

        if((compileForShell ? 1 : 0) +
                (compileForPython ? 1 : 0) +
                (compileForXml ? 1 : 0) +
                (compileForDebug ? 1 : 0) != 1) {
            throw new IllegalArgumentException("Must specify exactly one of " +
                    "-" + CMD_COMPILE_OP_SHELL + ", -" + CMD_COMPILE_OP_PYTHON +
                    ", -" + CMD_COMPILE_OP_XML + " and -" +
                    CMD_COMPILE_OP_DEBUG);
        }

        if(compileForShell) {
            return new ShellOracleLayoutPrinter(layout);
        } else if(compileForPython) {
            return new PythonOracleLayoutPrinter(layout);
        } else if(compileForXml) {
            return new XmlEntityLayoutPrinter(layout);
        } else {
            return new DebugLayoutPrinter(layout);
        }
    }

    /**
     * An ProblemConsumer adapter that maps errors to warnings.
     */
    private static class ErrorsAsWarningsProblemConsumer implements ProblemConsumer
    {
        ProblemConsumer _inner;

        ErrorsAsWarningsProblemConsumer( ProblemConsumer inner) {
            _inner = inner;
        }

        @Override
        public void setFilename( String name) {
            _inner.setFilename(name);
        }

        @Override
        public void setLineNumberReader( LineNumberReader reader) {
            _inner.setLineNumberReader(reader);
        }

        @Override
        public void error( String message) {
            _inner.warning(message);
        }

        @Override
        public void warning( String message) {
            _inner.warning(message);
        }

        @Override
        public void info( String message) {
            _inner.info(message);
        }
    }

    /**
     * Provide a ProblemConsumer that logs all messages to standard output and
     * doesn't terminate the parsing process if an error is discovered.  It
     * can also provide a one-line summary describing the number of warnings
     * and errors encountered.
     */
    private static class OutputStreamProblemConsumer extends DefaultProblemConsumer
    {
        private int _errors;
        private int _warnings;
        private final PrintStream _out;

        public OutputStreamProblemConsumer(OutputStream out)
        {
            _out = new PrintStream(out);
        }

        @Override
        public void error(String message)
        {
            _out.println("[ERROR] " + addContextTo(message));
            _errors++;
        }

        @Override
        public void warning(String message)
        {
            _out.println("[WARNING] " + addContextTo(message));
            _warnings++;
        }

        @Override
        public void info(String message)
        {
            _out.println("[INFO] " + addContextTo(message));
        }

        public int getReturnCode()
        {
            if (_errors > 0) {
                return 2;
             } else if (_warnings > 0) {
                return 1;
             } else {
                return 0;
             }
        }

        public void printSummary()
        {
            if( _warnings == 0 && _errors == 0) {
                System.out.println("No problems found.");
            } else {
                System.out.println(buildProblemsMessage());
            }
        }

        private String buildProblemsMessage()
        {
            StringBuilder sb = new StringBuilder();

            sb.append("Found ");
            cardinalMessage(sb, _errors, "error");

            if(_warnings > 0 && _errors > 0) {
                sb.append(" and ");
            }

            cardinalMessage(sb, _warnings, "warning");
            sb.append(".");
            return sb.toString();
        }

        private void cardinalMessage(StringBuilder sb, int count, String label)
        {
            switch(count) {
            case 0:
                break;
            case 1:
                sb.append("1 ").append(label);
                break;
            default:
                sb.append(count).append(" ").append(label).append("s");
                break;
            }
        }
    }
}
