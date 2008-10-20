package dmg.cells.nucleus;

import java.util.Enumeration;
import java.util.List;
import java.util.ArrayList;
import java.util.Formatter;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Map;
import java.util.HashMap;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.log4j.Level;
import org.apache.log4j.Priority;
import org.apache.log4j.Appender;
import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.spi.LoggerRepository;

import dmg.util.Args;

/**
 * Sort loggers according to name. With the Hierarchy
 * RepositoryLogger, this happens to order loggers according to the
 * hierarchy (children below parent).
 */
class HierarchySort implements Comparator<Logger>
{
    public int compare(Logger a, Logger b)
    {
        if (a == a.getRootLogger()) {
            return -1;
        } else if (b == b.getRootLogger()) {
            return 1;
        } else {
            return a.getName().compareTo(b.getName());
        }
    }

    public boolean equals(Logger a, Logger b)
    {
        return a.getName().equals(b.getName());
    }
}

public class Log4jShell
{
    public Log4jShell()
    {

    }

    private Logger getLogger(String name)
    {
        return name.equals("root")
            ? LogManager.getRootLogger()
            : LogManager.getLogger(name);
    }

    private List<Logger> getLoggers()
    {
        Enumeration loggers = LogManager.getCurrentLoggers();
        List<Logger> result = new ArrayList();
        result.add(LogManager.getRootLogger());
        while (loggers.hasMoreElements()) {
            result.add((Logger) loggers.nextElement());
        }
        return result;
    }

    private Map<String,Appender> getAppenders()
    {
        Map<String,Appender> appenders = new HashMap();
        for (Logger logger: getLoggers()) {
            Enumeration e = logger.getAllAppenders();
            while (e.hasMoreElements()) {
                Appender appender = (Appender) e.nextElement();
                appenders.put(appender.getName(), appender);
            }
        }
        return appenders;
    }

    private List<Appender> getAppenders(Logger logger)
    {
        Enumeration appenders = logger.getAllAppenders();
        List<Appender> result = new ArrayList();
        while (appenders.hasMoreElements()) {
            result.add((Appender) appenders.nextElement());
        }
        return result;
    }

    private List<String> getNames(List<Appender> appenders)
    {
        List<String> result = new ArrayList(appenders.size());
        for (Appender appender: appenders) {
            result.add(appender.getName());
        }
        return result;
    }

    private Level toLevel(String level)
    {
        if (level.equals("-")) {
            return null;
        } else if (level.equals("OFF")) {
            return Level.OFF;
        } else if (level.equals("FATAL")) {
            return Level.FATAL;
        } else if (level.equals("ERROR")) {
            return Level.ERROR;
        } else if (level.equals("WARN")) {
            return Level.WARN;
        } else if (level.equals("INFO")) {
            return Level.INFO;
        } else if (level.equals("DEBUG")) {
            return Level.DEBUG;
        } else if (level.equals("ALL")) {
            return Level.ALL;
        }
        throw new IllegalArgumentException("No such level: " + level);
    }

    private String toString(Priority level)
    {
        return (level == null) ? "-" : level.toString();
    }

    public final static String hh_log4j_logger_ls = "[-a]";
    public final static String fh_log4j_logger_ls =
        "Lists logger instances. Loggers that inherit all properties are\n" +
        "not listed unless the -a option is specified.";
    public String ac_log4j_logger_ls(Args args)
    {
        final String format = "%-5s %-30s %s\n";

        boolean all = (args.getOpt("a") != null);
        Formatter f = new Formatter();
        f.format(format, "Level", "Appenders", "Logger");
        f.format(format, "-----", "---------", "------");
        List<Logger> loggers = getLoggers();
        Collections.sort(loggers, new HierarchySort());
        for (Logger logger: loggers) {
            List<Appender> appenders = getAppenders(logger);
            boolean hasAppenders = !appenders.isEmpty();
            boolean isEndOfRoad = !logger.getAdditivity();
            boolean hasLevel = (logger.getLevel() != null);
            boolean isRoot = (logger.getName().equals("root"));
            if (all || hasAppenders || isEndOfRoad || hasLevel || isRoot) {
                f.format(format,
                         toString(logger.getLevel()),
                         getNames(appenders),
                         logger.getName());
            }
        }
        return f.toString();
    }

    public final static String hh_log4j_logger_set =
        "<logger> -|OFF|FATAL|ERROR|WARN|INFO|DEBUG|ALL";
    public final static String fh_log4j_logger_set =
        "Sets log level of <logger>. A hyphen indicates that the log level\n" +
        "is inherited from the parent logger. Remember to quote the hyphen.";
    public String ac_log4j_logger_set_$_2(Args args)
    {
        String name = args.argv(0);
        String level = args.argv(1).toUpperCase();
        Logger logger = getLogger(name);
        if (logger == null)
            return "Logger not found: " + name;

        logger.setLevel(toLevel(level));
        return "Log level of " + name + " set to " + level;
    }

    public final static String hh_log4j_logger_attach =
        "<logger> <appender>";
    public final static String fh_log4j_logger_attach =
        "Attach <logger> to output module <appender>.";
    public String ac_log4j_logger_attach_$_2(Args args)
    {
        String name = args.argv(0);
        String appender = args.argv(1);
        Logger logger = getLogger(name);
        if (logger == null)
            return "Logger not found: " + name;

        Appender app = getAppenders().get(appender);
        if (app == null)
            return "Appender not found: " + app;

        logger.addAppender(app);

        return name + " attached to " + appender;
    }

    public final static String hh_log4j_logger_detach =
        "<logger> <appender>";
    public final static String fh_log4j_logger_detach =
        "Detach <logger> from output module <appender>.";
    public String ac_log4j_logger_detach_$_2(Args args)
    {
        String name = args.argv(0);
        String appender = args.argv(1);
        Logger logger = getLogger(name);
        if (logger == null)
            return "Logger not found: " + name;

        logger.removeAppender(appender);

        return name + " detached from " + appender;
    }

    public final static String fh_log4j_appender_ls =
        "Lists all output modules.";
    public String ac_log4j_appender_ls(Args args)
    {
        final String format = "%-15s %s\n";
        Formatter f = new Formatter();
        f.format(format, "Appender", "Threshold");
        f.format(format, "--------", "---------");
        for (Appender appender: getAppenders().values()) {
            if (appender instanceof AppenderSkeleton) {
                Priority priority =
                    ((AppenderSkeleton) appender).getThreshold();
                f.format(format, appender.getName(), toString(priority));
            } else {
                f.format(format, appender.getName(), "-");
            }
        }
        return f.toString();
    }

    public final static String hh_log4j_appender_set =
        "<appender> -|OFF|FATAL|ERROR|WARN|INFO|DEBUG|ALL";
    public final static String fh_log4j_appender_set =
        "Sets the threshold of <appender>. Each output module can have a\n" +
        "filter threshold. Messages below the threshold are not logged.";
    public String ac_log4j_appender_set_$_2(Args args)
    {
        String name = args.argv(0);
        String level = args.argv(1);
        Appender appender = getAppenders().get(name);
        if (name == null)
            return "Appender not found: " + name;

        if (!(appender instanceof AppenderSkeleton))
            return "Appender cannot have a threshold";

        ((AppenderSkeleton) appender).setThreshold(toLevel(level));
        return "Appender threshold of " + name + " set to " + level;
    }
}