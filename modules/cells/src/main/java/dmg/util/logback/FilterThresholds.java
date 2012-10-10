package dmg.util.logback;

import ch.qos.logback.classic.Level;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.Collections;
import java.util.Comparator;
import java.util.concurrent.ExecutionException;

import static com.google.common.base.Preconditions.*;

/**
 * This class maintains filter thresholds. Two types of inheritance
 * are supported:
 *
 * - Inheritance from a parent FilterThresholds
 * - Inheritance from parent loggers
 *
 * If a threshold is not defined for a given combination of logger and
 * appender, then the parent FilterThresholds is consulted recursively.
 * If not defined in the parent, the threshold of the parent logger is
 * used recursively.
 */
public class FilterThresholds
{
    private final FilterThresholds _parent;

    private final Set<String> _appenders = Sets.newHashSet();

    /* Logger x Appender -> Level */
    private final Map<LoggerName,Map<String,Level>> _rules = Maps.newHashMap();

    /* Logger x Appender -> Level */
    private final LoadingCache<LoggerName,Map<String,Level>> _effectiveMaps =
            CacheBuilder.newBuilder().build(
                    new CacheLoader<LoggerName,Map<String,Level>>()
                    {
                        @Override
                        public Map<String, Level> load(LoggerName logger)
                        {
                            return computeEffectiveMap(logger);
                        }
                    });

    /* Logger -> Level */
    private final LoadingCache<LoggerName,Optional<Level>> _effectiveLevels =
            CacheBuilder.newBuilder().build(
                    new CacheLoader<LoggerName,Optional<Level>>()
                    {
                        @Override
                        public Optional<Level> load(LoggerName logger)
                        {
                            try {
                                Map<String,Level> map = _effectiveMaps.get(logger);
                                return map.isEmpty()
                                        ? Optional.<Level>absent()
                                        : Optional.of(Collections.min(map.values(), LEVEL_ORDER));
                            } catch (ExecutionException e) {
                                throw Throwables.propagate(e.getCause());
                            }
                        }
                    });

    private static final Comparator<Level> LEVEL_ORDER =
        new Comparator<Level>() {
            @Override
            public int compare(Level o1, Level o2)
            {
                if (!o1.isGreaterOrEqual(o2)) {
                    return -1;
                } else if (!o2.isGreaterOrEqual(o1)) {
                    return 1;
                } else {
                    return 0;
                }
            }
        };

    public FilterThresholds()
    {
        this(null);
    }

    public FilterThresholds(FilterThresholds parent)
    {
        _parent = parent;
    }

    public synchronized void addAppender(String name)
    {
        checkNotNull(name);
        _appenders.add(name);
    }

    public synchronized Collection<String> getAppenders()
    {
        if (_parent == null) {
            return Lists.newArrayList(_appenders);
        } else {
            Collection<String> appenders = _parent.getAppenders();
            appenders.addAll(_appenders);
            return appenders;
        }
    }

    public synchronized boolean hasAppender(String appender)
    {
        return _appenders.contains(appender) ||
            (_parent != null && _parent.hasAppender(appender));
    }

    public synchronized Level get(LoggerName logger, String appender)
    {
        return getMap(logger).get(appender);
    }

    public synchronized void setThreshold(LoggerName logger, String appender, Level level)
    {
        checkNotNull(logger);
        checkNotNull(level);
        checkArgument(hasAppender(appender));

        Map<String,Level> map = _rules.get(logger);
        if (map == null) {
            map = Maps.newHashMap();
            _rules.put(logger, map);
        }
        map.put(appender, level);

        clearCache();
    }

    public synchronized void remove(LoggerName logger, String appender)
    {
        Map<String,Level> map = _rules.get(logger);
        if (map != null) {
            map.remove(appender);
            if (map.isEmpty()) {
                _rules.remove(logger);
            }
            clearCache();
        }
    }

    public synchronized void clear()
    {
        _rules.clear();
        clearCache();
    }

    private void clearCache()
    {
        _effectiveMaps.invalidateAll();
        _effectiveLevels.invalidateAll();
    }

    /**
     * Returns a map from appenders to levels for a logger.
     *
     * Neither the parent levels nor parent loggers are consulted.
     */
    private synchronized Map<String,Level> getMap(LoggerName logger)
    {
        Map<String,Level> map = _rules.get(logger);
        return (map == null) ? Collections.EMPTY_MAP : map;
    }

    /**
     * Returns a map from appenders to levels for a logger.
     *
     * The map contains inherited levels from parent levels.
     */
    public synchronized Map<String,Level> getInheritedMap(LoggerName logger)
    {
        if (_parent == null) {
            return Maps.newHashMap(getMap(logger));
        } else {
            Map<String,Level> map = _parent.getInheritedMap(logger);
            map.putAll(getMap(logger));
            return map;
        }
    }

    /**
     * Returns a map from appenders to levels for a logger.
     *
     * The map contains the effective log levels, that is, the levels
     * used for filtering log events.
     */
    private synchronized Map<String,Level> computeEffectiveMap(LoggerName logger)
    {
        LoggerName parent = logger.getParent();
        Map<String,Level> map;
        if (parent == null) {
            map = getInheritedMap(logger);
        } else {
            map = computeEffectiveMap(parent);
            map.putAll(getInheritedMap(logger));
        }
        return map;
    }

    /**
     * Returns the effectice log level for a given pair of logger and appender.
     */
    public Level getThreshold(LoggerName logger, String appender)
    {
        try {
            return _effectiveMaps.get(logger).get(appender);
        } catch (ExecutionException e) {
            throw Throwables.propagate(e.getCause());
        }
    }

    public Level getThreshold(LoggerName logger)
    {
        try {
            return _effectiveLevels.get(logger).orNull();
        } catch (ExecutionException e) {
            throw Throwables.propagate(e.getCause());
        }
    }
}
