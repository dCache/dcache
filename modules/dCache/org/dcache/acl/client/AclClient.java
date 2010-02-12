package org.dcache.acl.client;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.MissingArgumentException;
import org.apache.commons.cli.MissingOptionException;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.UnrecognizedOptionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AclClient {

    private static final Logger logger = LoggerFactory.getLogger("logger.org.dcache.authorization." + AclClient.class.getName());

    protected final static String LINE_SEPARATOR = System.getProperty("line.separator", "\n");

    public static final String OPTION_HELP = "h";

    protected String[] _args;

    public AclClient(String[] args) {
        init(args, false);
    }

    public AclClient(String[] args, boolean stopAtNonOption) {
        init(args, stopAtNonOption);
    }

    private void exitHelp(Options opts, int exitValue) {
        HelpFormatter help = new HelpFormatter();
        help.printHelp("java " + getClass().getName() + " path", opts);
        System.exit(exitValue);
    }

    private void exitHelp(Options opts, int exitValue, String message) {
        if ( exitValue == 0 )
            logger.info(message.toString());
        else
            logger.error(message.toString());
        exitHelp(opts, exitValue);
    }

    void exitHelp(int exitValue) {
        HelpFormatter help = new HelpFormatter();
        help.printHelp("java " + getClass().getName(), createOptions());
        System.exit(exitValue);
    }

    void exitHelp(int exitValue, String message) {
        if ( exitValue == 0 )
            logger.info(message.toString());
        else
            logger.error(message.toString());
        exitHelp(exitValue);
    }

    Options createOptions() {
        Options opts = new Options();
        boolean hasArg = true;

        Option help = new Option(OPTION_HELP, "help", !hasArg, "Print help for this application");
        help.setRequired(false);
        opts.addOption(help);

        return opts;
    }

    private void init(String[] args, boolean stopAtNonOption) {
        Options opts = createOptions();
        try {
            // parse the command line arguments
            GnuParser parser = new GnuParser();
            CommandLine cmd = parser.parse(opts, args, stopAtNonOption);

            if ( cmd.hasOption(OPTION_HELP) )
                exitHelp(opts, 0);
            else
                _args = cmd.getArgs();

        } catch (MissingOptionException moe) {
            exitHelp(opts, 1, "Missing Option Exception: " + moe.getMessage());

        } catch (MissingArgumentException mae) {
            exitHelp(opts, 1, "Missing Argument Exception: " + mae.getMessage());

        } catch (UnrecognizedOptionException uoe) {
            exitHelp(opts, 1, "Unrecognized Option Exception: " + uoe.getMessage());

        } catch (Exception e) {
            exitHelp(opts, 1, "Exception: " + e.getMessage());
        }
    }

    public String[] getArgs() {
        return _args;
    }

    public void setArgs(String[] args) {
        _args = args;
    }

    String fpath2rsId(String path) {

        // XXX: Unused !
        // String rsID = null;
        // try {
        // File aFile = new File(path);
        // FPathHandler fPath = new FPathHandler();
        // rsID = fPath.getID(aFile.getCanonicalPath());

        // } catch (Exception Ignore) {}

        // if ( rsID != null )
        // return rsID;

        return path;
    }
}
