package org.dcache.pool.migration;


import org.dcache.cells.CellStub;
import org.dcache.cells.MessageCallback;
import diskCacheV111.vehicles.PoolManagerGetPoolsMessage;
import diskCacheV111.vehicles.PoolManagerPoolInformation;

import java.util.List;
import java.util.Collection;
import java.util.ArrayList;
import java.util.Collections;
import java.util.regex.Pattern;

import dmg.cells.nucleus.CellPath;

import org.apache.commons.jexl2.Expression;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class PoolListFromPoolManager
    implements RefreshablePoolList,
               MessageCallback<PoolManagerGetPoolsMessage>
{
    private static final Logger _log =
        LoggerFactory.getLogger(PoolListFromPoolManager.class);

    protected final Collection<Pattern> _exclude;
    protected final Expression _excludeWhen;
    protected final Collection<Pattern> _include;
    protected final Expression _includeWhen;
    protected List<PoolManagerPoolInformation> _pools = Collections.emptyList();

    public PoolListFromPoolManager(Collection<Pattern> exclude,
                                   Expression excludeWhen,
                                   Collection<Pattern> include,
                                   Expression includeWhen)
    {
        _exclude = exclude;
        _excludeWhen = excludeWhen;
        _include = include;
        _includeWhen = includeWhen;
    }

    synchronized public List<PoolManagerPoolInformation> getPools()
    {
        return _pools;
    }

    synchronized protected void setPools(List<PoolManagerPoolInformation> pools)
    {
        _pools = Collections.unmodifiableList(pools);
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

    public void success(PoolManagerGetPoolsMessage msg)
    {
        List<PoolManagerPoolInformation> pools =
            new ArrayList<PoolManagerPoolInformation>(msg.getPools().size());
        for (PoolManagerPoolInformation pool: msg.getPools()) {
            if (!isExcluded(pool) && isIncluded(pool)) {
                pools.add(pool);
            }
        }
        setPools(pools);
    }

    public void failure(int rc, Object error)
    {
        _log.error("Failed to query pool manager "
                   + error + ")");
    }

    public void noroute()
    {
        _log.error("No route to pool manager");
    }

    public void timeout()
    {
        _log.error("Pool manager timeout");
    }

    private boolean evaluate(Expression expression,
                             PoolManagerPoolInformation pool)
    {
        MapContextWithConstants context = new MapContextWithConstants();
        context.addConstant("target", new PoolValues(pool));

        Object result = expression.evaluate(context);
        if (!(result instanceof Boolean)) {
            _log.error(expression.getExpression() +
                       ": The expression does not evaluate to a boolean");
        }

        /* Notice that the following round-about code also happens to
         * work if result is not a Boolean.
         */
        return Boolean.TRUE.equals(result);
    }
}
