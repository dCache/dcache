package dmg.util.logback;

import ch.qos.logback.classic.Level;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.util.Collection;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.HashSet;
import java.util.Collections;
import java.util.Comparator;

/**
 * Implementation of FilterThresholds that supports to types of
 * inheritance:
 *
 * - Inheritance from a parent FilterThresholds
 * - Inheritance from parent loggers
 *
 * If a threshold is not defined for a given combination of logger and
 * filter then the parent InheritableFilterThresholds is consulted
 * recursively. If not defined in the parent then threshold of the
 * parent logger is used recursively.
 */
public class FilterThresholds
{
    private final FilterThresholds _parent;

    private final Set<String> _filters = new HashSet<String>();

    /* Logger x Filter -> Level */
    private final Map<LoggerName,Map<String,Level>> _rules =
        new HashMap<LoggerName,Map<String,Level>>();

    /* Logger x Filter -> Level */
    private final Map<LoggerName,Map<String,Level>> _effectiveMaps =
        new HashMap<LoggerName,Map<String,Level>>();

    /* Logger -> Level */
    private final Map<LoggerName,Level> _effectiveLevels =
        new HashMap<LoggerName,Level>();

    private static final Comparator<Level> LEVEL_ORDER =
        new Comparator<Level>() {
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

    public synchronized void addFilter(String filter)
    {
        if (filter == null) {
            throw new IllegalArgumentException("Null value not allowed");
        }
        _filters.add(filter);
    }

    public synchronized Collection<String> getFilters()
    {
        if (_parent == null) {
            return new ArrayList<String>(_filters);
        } else {
            Collection<String> filters = _parent.getFilters();
            filters.addAll(_filters);
            return filters;
        }
    }

    public synchronized boolean hasFilter(String filter)
    {
        return _filters.contains(filter) ||
            (_parent != null && _parent.hasFilter(filter));
    }

    public synchronized Level get(LoggerName logger, String filter)
    {
        return getMap(logger).get(filter);
    }

    public synchronized void setThreshold(LoggerName logger, String filter, Level level)
    {
        if (logger == null || filter == null || level == null) {
            throw new IllegalArgumentException("Null value not allowed");
        }

        if (!hasFilter(filter)) {
            throw new IllegalArgumentException("Filter does not exist");
        }

        Map<String,Level> map = _rules.get(logger);
        if (map == null) {
            map = new HashMap<String,Level>();
            _rules.put(logger, map);
        }
        map.put(filter, level);

        clearCache();
    }

    public synchronized void remove(LoggerName logger, String filter)
    {
        Map<String,Level> map = _rules.get(logger);
        if (map != null) {
            map.remove(filter);
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

    private synchronized void clearCache()
    {
        _effectiveLevels.clear();
        _effectiveMaps.clear();
    }

    /**
     * Returns a map from filters to levels for a logger.
     *
     * Neither the parent levels nor parent loggers are consulted.
     */
    private synchronized Map<String,Level> getMap(LoggerName logger)
    {
        Map<String,Level> map = _rules.get(logger);
        return (map == null) ? Collections.EMPTY_MAP : map;
    }

    /**
     * Returns a map from filters to levels for a logger.
     *
     * The map contains inherited levels from parent levels.
     */
    public synchronized Map<String,Level> getInheritedMap(LoggerName logger)
    {
        if (_parent == null) {
            return new HashMap<String,Level>(getMap(logger));
        } else {
            Map<String,Level> map = _parent.getInheritedMap(logger);
            map.putAll(getMap(logger));
            return map;
        }
    }

    /**
     * Returns a map from filters to levels for a logger.
     *
     * The map contains the effective log levels, that is, the levels
     * used for filtering log events.
     */
    private synchronized Map<String,Level> getEffectiveMap(LoggerName logger)
    {
        Map<String,Level> map = _effectiveMaps.get(logger);
        if (map == null) {
            LoggerName parent = logger.getParent();
            if (parent == null) {
                map = getInheritedMap(logger);
            } else {
                map = new HashMap<String,Level>(getEffectiveMap(parent));
                map.putAll(getInheritedMap(logger));
            }
            _effectiveMaps.put(logger, map);
        }
        return map;
    }

    /**
     * Returns a map from filters to levels for a logger.
     *
     * The map contains the effective log levels, that is, the levels
     * used for filtering log events.
     */
    public synchronized Level getThreshold(LoggerName logger, String filter)
    {
        return getEffectiveMap(logger).get(filter);
    }

    public synchronized Level getThreshold(LoggerName logger)
    {
        if (_effectiveLevels.containsKey(logger)) {
            return _effectiveLevels.get(logger);
        }

        Map<String,Level> map = getEffectiveMap(logger);
        Level level =
            map.isEmpty() ? null : Collections.min(map.values(), LEVEL_ORDER);
        _effectiveLevels.put(logger, level);
        return level;
    }
}
