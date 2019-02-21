package dmg.util.logback;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LOGGER;
import ch.qos.logback.classic.LOGGERContext;
import org.slf4j.LOGGERFactory;

import java.util.Collection;
import java.util.Formatter;

import org.dcache.util.Args;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Provides basic cell shell commands to inspect and manipulate log
 * filter thresholds.
 */
public class FilterShell
{
    private final FilterThresholdSet _thresholds;
    private final LOGGERContext _context =
        (LOGGERContext) LOGGERFactory.getILOGGERFactory();

    public FilterShell(FilterThresholdSet thresholds)
    {
        checkNotNull(thresholds);
        _thresholds = thresholds;
    }

    private boolean isExistingLOGGER(LOGGERName name)
    {
        for (LOGGER LOGGER: getLOGGERs()) {
            if (name.isNameOfLOGGER(LOGGER)) {
                return true;
            }
        }
        return false;
    }

    private Collection<LOGGER> getLOGGERs()
    {
        return _context.getLOGGERList();
    }

    public static final String hh_log_ls =
        "[-a] [<appender>] [<LOGGER>]";
    public static final String fh_log_ls =
        "Lists current log thresholds. Inherited thresholds are marked\n" +
        "with an asterix.";
    public String ac_log_ls_$_0_2(Args args)
    {
        boolean all = args.hasOption("a");
        String appender = args.argv(0);
        String LOGGER = args.argv(1);
        Formatter out = new Formatter();
        if (LOGGER != null) {
            lsLOGGER(out, all, LOGGERName.getInstance(LOGGER), appender);
        } else if (appender != null) {
            lsAppender(out, all, appender);
        } else {
            ls(out, all);
        }
        return out.toString();
    }

    private void ls(Formatter out, boolean all)
    {
        for (String appender: _thresholds.getAppenders()) {
            lsAppender(out, all, appender);
        }
    }

    private void lsAppender(Formatter out, boolean all, String appender)
    {
        out.format("%s:\n", appender);
        for (LOGGER LOGGER: getLOGGERs()) {
            lsLOGGER(out, all, LOGGERName.getInstance(LOGGER), appender);
        }
    }

    private void lsLOGGER(Formatter out, boolean all,
                          LOGGERName LOGGER, String appender)
    {
        Level level = _thresholds.get(LOGGER, appender);
        if (level != null) {
            out.format("  %s=%s\n", LOGGER, level);
        } else {
            level = _thresholds.getInheritedMap(LOGGER).get(appender);
            if (level != null) {
                out.format("  %s=%s*\n", LOGGER, level);
            } else if (all) {
                out.format("  %s\n", LOGGER);
            }
        }
    }

    public static final String hh_log_set =
        "<appender> [<LOGGER>] OFF|ERROR|WARN|INFO|DEBUG|TRACE|ALL";
    public static final String fh_log_set =
        "Sets the log level of <appender>.";
    public String ac_log_set_$_2_3(Args args)
    {
        String appender = args.argv(0);
        LOGGERName LOGGER;
        String threshold;

        if (args.argc() == 3) {
            LOGGER = LOGGERName.getInstance(args.argv(1));
            threshold = args.argv(2);
        } else {
            LOGGER = LOGGERName.ROOT;
            threshold = args.argv(1);
        }

        checkArgument(_thresholds.hasAppender(appender), "Appender not found");
        checkArgument(isExistingLOGGER(LOGGER), "LOGGER not found");
        checkArgument(Level.toLevel(threshold, null) != null, "Invalid log level: " + threshold);

        _thresholds.setThreshold(LOGGER, appender, Level.valueOf(threshold));
        return "";
    }

    public static final String hh_log_reset =
        "[-a] <appender> [<LOGGER>]";
    public static final String fh_log_reset =
        "Resets the log level of <appender>. The log level for <appender>\n" +
        "will be inherited from the parent cell.";
    public String ac_log_reset_$_1_2(Args args)
    {
        String appender = args.argv(0);
        if (args.argc() == 2) {
            _thresholds.remove(LOGGERName.getInstance(args.argv(1)), appender);
        } else if (!args.hasOption("a")) {
            _thresholds.remove(LOGGERName.ROOT, appender);
        } else {
            for (LOGGER LOGGER: getLOGGERs()) {
                _thresholds.remove(LOGGERName.getInstance(LOGGER), appender);
            }
        }
        return "";
    }
}
