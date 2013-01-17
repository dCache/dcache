package org.dcache.pool.migration;

import java.util.List;
import java.util.Collection;
import java.util.regex.Pattern;

import diskCacheV111.vehicles.PoolManagerPoolInformation;

import org.dcache.util.expression.Expression;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;

/**
 * RefreshablePoolList decorator that can filter the list of pools.
 */
public class PoolListFilter implements RefreshablePoolList
{
    private static final Logger _log =
        LoggerFactory.getLogger(PoolListFilter.class);

    private static final ImmutableList<PoolManagerPoolInformation> EMPTY_LIST =
        ImmutableList.of();

    private final Collection<Pattern> _exclude;
    private final Expression _excludeWhen;
    private final Collection<Pattern> _include;
    private final Expression _includeWhen;
    private final RefreshablePoolList _sourceList;
    private final RefreshablePoolList _poolList;

    private ImmutableList<PoolManagerPoolInformation> _cachedList;
    private ImmutableList<PoolManagerPoolInformation> _filteredList;

    public PoolListFilter(RefreshablePoolList poolList,
                          Collection<Pattern> exclude,
                          Expression excludeWhen,
                          Collection<Pattern> include,
                          Expression includeWhen,
                          RefreshablePoolList sourceList)
    {
        _poolList = poolList;
        _exclude = exclude;
        _excludeWhen = excludeWhen;
        _include = include;
        _includeWhen = includeWhen;
        _sourceList = sourceList;
    }

    @Override
    public boolean isValid()
    {
        return _sourceList.isValid() && _poolList.isValid();
    }

    @Override
    public void refresh()
    {
        _poolList.refresh();
    }

    @Override
    synchronized public
        ImmutableList<PoolManagerPoolInformation> getPools()
    {
        if (!isValid()) {
            return EMPTY_LIST;
        }

        ImmutableList<PoolManagerPoolInformation> list = _poolList.getPools();
        if (!list.equals(_cachedList)) {
            ImmutableList.Builder<PoolManagerPoolInformation> filteredList =
                ImmutableList.builder();
            for (PoolManagerPoolInformation pool: list) {
                if (!isExcluded(pool) && isIncluded(pool)) {
                    filteredList.add(pool);
                }
            }
            _filteredList = filteredList.build();
        }
        _cachedList = list;
        return _filteredList;
    }

    private boolean matchesAny(Collection<Pattern> patterns, String s)
    {
        for (Pattern pattern: patterns) {
            if (pattern.matcher(s).matches()) {
                return true;
            }
        }
        return false;
    }

    private boolean isExcluded(PoolManagerPoolInformation pool)
    {
        if (matchesAny(_exclude, pool.getName())) {
            return true;
        }
        return evaluate(_excludeWhen, pool);
    }

    private boolean isIncluded(PoolManagerPoolInformation pool)
    {
        if (!_include.isEmpty()) {
            return matchesAny(_include, pool.getName());
        }
        return evaluate(_includeWhen, pool);
    }

    private PoolManagerPoolInformation getSource()
    {
        List<PoolManagerPoolInformation> list = _sourceList.getPools();
        if (list.size() != 1) {
            throw new IllegalStateException("Unexpected source pool list: Exactly one item was expected, but it contained " + list.size());
        }
        return list.get(0);
    }

    private boolean evaluate(Expression expression,
                             PoolManagerPoolInformation pool)
    {
        SymbolTable symbols = new SymbolTable();
        symbols.put(MigrationModule.CONSTANT_TARGET, pool);
        symbols.put(MigrationModule.CONSTANT_SOURCE, getSource());
        return expression.evaluateBoolean(symbols);
    }

    @Override
    public String toString()
    {
        List<PoolManagerPoolInformation> pools = getPools();
        if (pools.isEmpty()) {
            return "";
        }

        StringBuilder s = new StringBuilder();
        s.append(pools.get(0).getName());
        for (int i = 1; i < pools.size(); i++) {
            s.append(',').append(pools.get(i).getName());
        }
        return s.toString();
    }
}
