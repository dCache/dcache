package dmg.cells.nucleus;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LOGGER;
import ch.qos.logback.classic.LOGGERContext;
import ch.qos.logback.classic.encoder.PatternLayoutEncoder;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Appender;
import ch.qos.logback.core.ConsoleAppender;
import ch.qos.logback.core.FileAppender;
import ch.qos.logback.core.encoder.Encoder;
import org.slf4j.LOGGERFactory;

import java.util.ArrayList;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.dcache.util.Args;

public class LogbackShell
{
    private LOGGERContext _context;

    public LogbackShell()
    {
        _context = (LOGGERContext) LOGGERFactory.getILOGGERFactory();
    }

    private LOGGER getLOGGER(String name)
    {
        return name.equals("root")
            ? _context.getLOGGER(LOGGER.ROOT_LOGGER_NAME)
            : _context.getLOGGER(name);
    }

    private List<LOGGER> getLOGGERS()
    {
        return _context.getLOGGERList();
    }

    private Map<String,Appender<ILoggingEvent>> getAppenders()
    {
        Map<String,Appender<ILoggingEvent>> appenders =
            new HashMap<>();
        for (LOGGER LOGGER: getLOGGERS()) {
            Iterator<Appender<ILoggingEvent>> i = LOGGER.iteratorForAppenders();
            while (i.hasNext()) {
                Appender<ILoggingEvent> appender = i.next();
                appenders.put(appender.getName(), appender);
            }
        }
        return appenders;
    }

    private List<Appender<ILoggingEvent>> getAppenders(LOGGER LOGGER)
    {
        Iterator<Appender<ILoggingEvent>> appenders =
            LOGGER.iteratorForAppenders();
        List<Appender<ILoggingEvent>> result =
            new ArrayList<>();
        while (appenders.hasNext()) {
            result.add(appenders.next());
        }
        return result;
    }

    private List<String> getNames(List<Appender<ILoggingEvent>> appenders)
    {
        List<String> result = new ArrayList<>(appenders.size());
        for (Appender<ILoggingEvent> appender: appenders) {
            result.add(appender.getName());
        }
        return result;
    }

    private String toString(Level level)
    {
        return (level == null) ? "" : level.toString();
    }

    public static final String hh_log_LOGGER_ls = "[-a]";
    public static final String fh_log_LOGGER_ls =
        "Lists LOGGER instances. LOGGERS that inherit all properties are\n" +
        "not listed unless the -a option is specified.";
    public String ac_log_LOGGER_ls(Args args)
    {
        final String format = "%-5s %-30s %s\n";

        boolean all = args.hasOption("a");
        Formatter f = new Formatter();
        f.format(format, "Level", "Appenders", "LOGGER");
        f.format(format, "-----", "---------", "------");
        for (LOGGER LOGGER: getLOGGERS()) {
            List<Appender<ILoggingEvent>> appenders = getAppenders(LOGGER);
            boolean hasAppenders = !appenders.isEmpty();
            boolean isEndOfRoad = !LOGGER.isAdditive();
            boolean hasLevel = (LOGGER.getLevel() != null);
            boolean isRoot = (LOGGER.getName().equals(LOGGER.ROOT_LOGGER_NAME));
            if (all || hasAppenders || isEndOfRoad || hasLevel || isRoot) {
                f.format(format,
                         toString(LOGGER.getLevel()),
                         getNames(appenders),
                         LOGGER.getName());
            }
        }
        return f.toString();
    }

    public static final String hh_log_LOGGER_set =
        "<LOGGER> OFF|ERROR|WARN|INFO|DEBUG|TRACE|ALL";
    public static final String fh_log_LOGGER_set =
        "Sets log level of <LOGGER>. Notice that the preferred method to\n" +
        "adjust log levels in dCache is to manipulate the appender log\n" +
        "levels through the 'log set' and 'log reset' commands.";
    public String ac_log_LOGGER_set_$_2(Args args)
    {
        String name = args.argv(0);
        Level level = Level.valueOf(args.argv(1));
        Logger LOGGER = getLogger(name);
        if (LOGGER == null) {
            throw new IllegalArgumentException("LOGGER not found: " + name);
        }

        LOGGER.setLevel(level);
        return "Log level of " + name + " set to " + level;
    }

    public static final String hh_log_LOGGER_reset =
        "<LOGGER>";
    public static final String fh_log_LOGGER_reset =
        "Resets the log level of <LOGGER>. The effective log level will be\n" +
        "inherited from the parent LOGGER. Notice that the preferred method\n" +
        "to adjust log levels in dCache is to manipulate the appender log\n" +
        "levels through the 'log set' and 'log reset' commands.";
    public String ac_log_LOGGER_reset_$_1(Args args)
    {
        String name = args.argv(0);
        Logger LOGGER = getLogger(name);
        if (LOGGER == null) {
            throw new IllegalArgumentException("LOGGER not found: " + name);
        }

        LOGGER.setLevel(null);
        return "Log level of " + name + " was reset";
    }

    public static final String hh_log_attach =
        "<LOGGER> <appender>";
    public static final String fh_log_attach =
        "Attach <LOGGER> to output module <appender>.";
    public String ac_log_attach_$_2(Args args)
    {
        String name = args.argv(0);
        String appender = args.argv(1);
        Logger LOGGER = getLogger(name);
        if (LOGGER == null) {
            throw new IllegalArgumentException("LOGGER not found: " + name);
        }

        Appender<ILoggingEvent> app = getAppenders().get(appender);
        if (app == null) {
            throw new IllegalArgumentException("Appender not found: " + appender);
        }

        LOGGER.addAppender(app);

        return name + " attached to " + appender;
    }

    public static final String hh_log_detach =
        "<LOGGER> <appender>";
    public static final String fh_log_detach =
        "Detach <LOGGER> from output module <appender>.";
    public String ac_log_detach_$_2(Args args)
    {
        String name = args.argv(0);
        String appender = args.argv(1);
        Logger LOGGER = getLogger(name);
        if (LOGGER == null) {
            throw new IllegalArgumentException("LOGGER not found: " + name);
        }

        LOGGER.detachAppender(appender);

        return name + " detached from " + appender;
    }

    public static final String hh_log_get_pattern =
            "<LOGGER> <appender>";
    public static final String fh_log_get_pattern =
            "Get encoder pattern for <LOGGER> <appender>.";
    public String ac_log_get_pattern_$_2(Args args)
    {
        String LoggerName = args.argv(0);
        String appenderName = args.argv(1);

        Logger LOGGER = getLogger(LoggerName);
        if (LOGGER == null) {
            throw new IllegalArgumentException("LOGGER not found: " + LoggerName);
        }

        Appender<ILoggingEvent> appender = LOGGER.getAppender(appenderName);
        if (appender == null) {
            throw new IllegalArgumentException("Appender not found: " + appenderName);
        }

        Encoder<?> encoder;
        if (appender instanceof ConsoleAppender) {
            encoder = ((ConsoleAppender<?>)appender).getEncoder();
        } else
        if (appender instanceof FileAppender) {
            encoder = ((FileAppender<?>)appender).getEncoder();
        } else {
            throw new IllegalArgumentException("Appender " + appenderName + " does not support encoders.");
        }
        PatternLayoutEncoder patternLayoutEncoder;
        if (encoder instanceof PatternLayoutEncoder) {
            patternLayoutEncoder = (PatternLayoutEncoder)encoder;
        } else {
            throw new IllegalArgumentException("Appender " + appenderName + " does not provide a pattern encoder.");
        }

        return "pattern of appender " + appenderName + " is " + patternLayoutEncoder.getPattern();
    }

    public static final String hh_log_set_pattern =
        "<LOGGER> <appender> <pattern>";
    public static final String fh_log_set_pattern =
        "Set encoder pattern to <pattern> for <LOGGER> <appender>.";
    public String ac_log_set_pattern_$_3(Args args)
    {
        String LoggerName = args.argv(0);
        String appenderName = args.argv(1);
        String pattern = args.argv(2);

        Logger LOGGER = getLogger(LoggerName);
        if (LOGGER == null) {
            throw new IllegalArgumentException("LOGGER not found: " + LoggerName);
        }

        Appender<ILoggingEvent> appender = LOGGER.getAppender(appenderName);
        if (appender == null) {
            throw new IllegalArgumentException("Appender not found: " + appenderName);
        }

        PatternLayoutEncoder encoder = new PatternLayoutEncoder();
        encoder.setContext(_context);
        encoder.setPattern(pattern);
        encoder.start();

        if (appender instanceof ConsoleAppender) {
            ((ConsoleAppender<ILoggingEvent>)appender).setEncoder(encoder);
        } else
        if (appender instanceof FileAppender) {
            ((FileAppender<ILoggingEvent>)appender).setEncoder(encoder);
        } else {
            throw new IllegalArgumentException("Appender " + appenderName + " does not support encoders");
        }

        return "pattern of appender " + LoggerName + '.' + appenderName + " set to " + encoder.getPattern();
    }
}
