package dmg.util.logback;

import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.Collection;
import java.util.Set;
import java.util.HashSet;
import java.util.Formatter;
import java.util.concurrent.ConcurrentHashMap;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;

import org.slf4j.LoggerFactory;

import dmg.util.Args;

/**
 * Provides basic cell shell commands to inspect and manipulate log
 * filter thresholds.
 *
 * Notice that there is a one-to-one correspondence between appenders
 * and filters. Hence we refer to appenders in the user interface, but
 * the code actually manipulates the filters attached to the
 * appenders.
 */
public class FilterShell
{
    private final FilterThresholds _thresholds;
    private final LoggerContext _context =
        (LoggerContext) LoggerFactory.getILoggerFactory();

    public FilterShell(FilterThresholds thresholds)
    {
        if (thresholds == null) {
            throw new IllegalArgumentException("Null argument is not valid");
        }
        _thresholds = thresholds;
    }

    private boolean isExistingLogger(LoggerName name)
    {
        for (Logger logger: getLoggers()) {
            if (name.isNameOfLogger(logger)) {
                return true;
            }
        }
        return false;
    }

    private Collection<Logger> getLoggers()
    {
        return _context.getLoggerList();
    }

    private Collection<String> getFilters()
    {
        return _thresholds.getFilters();
    }

    public final static String hh_log_ls =
        "[-a] [<appender>] [<logger>]";
    public final static String fh_log_ls =
        "Lists current log thresholds. Inherited thresholds are marked\n" +
        "with an asterix.";
    public String ac_log_ls_$_0_2(Args args)
    {
        boolean all = (args.getOpt("a") != null);
        String appender = args.argv(0);
        String logger = args.argv(1);
        Formatter out = new Formatter();
        if (logger != null) {
            lsLogger(out, all, LoggerName.getInstance(logger), appender);
        } else if (appender != null) {
            lsFilter(out, all, appender);
        } else {
            ls(out, all);
        }
        return out.toString();
    }

    private void ls(Formatter out, boolean all)
    {
        for (String filter: getFilters()) {
            lsFilter(out, all, filter);
        }
    }

    private void lsFilter(Formatter out, boolean all, String filter)
    {
        out.format("%s:\n", filter);
        for (Logger logger: getLoggers()) {
            lsLogger(out, all, LoggerName.getInstance(logger), filter);
        }
    }

    private void lsLogger(Formatter out, boolean all,
                          LoggerName logger, String filter)
    {
        Level level = _thresholds.get(logger, filter);
        if (level != null) {
            out.format("  %s=%s\n", logger, level);
        } else {
            level = _thresholds.getInheritedMap(logger).get(filter);
            if (level != null) {
                out.format("  %s=%s*\n", logger, level);
            } else if (all) {
                out.format("  %s\n", logger);
            }
        }
    }

    public final static String hh_log_set =
        "<appender> [<logger>] OFF|ERROR|WARN|INFO|DEBUG|TRACE|ALL";
    public final static String fh_log_set =
        "Sets the log level of <appender>.";
    public String ac_log_set_$_2_3(Args args)
    {
        String appender = args.argv(0);
        LoggerName logger;
        String threshold;

        if (args.argc() == 3) {
            logger = LoggerName.getInstance(args.argv(1));
            threshold = args.argv(2);
        } else {
            logger = LoggerName.ROOT;
            threshold = args.argv(1);
        }

        if (!getFilters().contains(appender)) {
            throw new IllegalArgumentException("Appender not found");
        }

        if (!isExistingLogger(logger)) {
            throw new IllegalArgumentException("Logger not found");
        }

        _thresholds.setThreshold(logger, appender, Level.valueOf(threshold));
        return "";
    }

    public final static String hh_log_reset =
        "[-a] <appender> [<logger>]";
    public final static String fh_log_reset =
        "Resets the log level of <appender>. The log level for <appender>\n" +
        "will be inherited from the parent cell.";
    public String ac_log_reset_$_1_2(Args args)
    {
        String appender = args.argv(0);
        if (args.argc() == 2) {
            _thresholds.remove(LoggerName.getInstance(args.argv(1)), appender);
        } else if (args.getOpt("a") == null) {
            _thresholds.remove(LoggerName.ROOT, appender);
        } else {
            for (Logger logger: getLoggers()) {
                _thresholds.remove(LoggerName.getInstance(logger), appender);
            }
        }
        return "";
    }
}