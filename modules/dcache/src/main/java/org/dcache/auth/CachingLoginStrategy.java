package org.dcache.auth;

import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.CheckedFuture;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.UncheckedExecutionException;

import javax.security.auth.Subject;

import java.io.PrintWriter;
import java.security.Principal;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.TimeoutCacheException;

import dmg.cells.nucleus.CellCommandListener;
import dmg.cells.nucleus.CellInfo;
import dmg.cells.nucleus.CellInfoProvider;

import org.dcache.util.Args;

/**
 * Caching implementation of {@link LoginStrategy}.
 */
public class CachingLoginStrategy implements LoginStrategy, CellCommandListener, CellInfoProvider
{
    private final LoginStrategy _inner;

    private final LoadingCache<Principal,CheckedFuture<Principal, CacheException>> _forwardCache;
    private final LoadingCache<Principal,CheckedFuture<Set<Principal>, CacheException>> _reverseCache;
    private final LoadingCache<Subject, CheckedFuture<LoginReply, CacheException>> _loginCache;

    private final long _time;
    private final TimeUnit _unit;
    private final int _size;

    /**
     * Create an instance of LoginStrategy
     *
     * @param inner {@link LoginStrategy} used for fetching data.
     * @param size maximal size of cached entries per cache table
     * @param timeout cache entry life time.
     * @param unit the time unit of the timeout argument
     */
    public CachingLoginStrategy(LoginStrategy inner, int size, long timeout, TimeUnit unit) {

        _inner = inner;

        _forwardCache = CacheBuilder.newBuilder()
                .expireAfterWrite(timeout, unit)
                .maximumSize(size)
                .softValues()
                .recordStats()
                .build( new ForwardFetcher());

        _reverseCache = CacheBuilder.newBuilder()
                .expireAfterWrite(timeout, unit)
                .maximumSize(size)
                .softValues()
                .recordStats()
                .build( new ReverseFetcher());

        _loginCache = CacheBuilder.newBuilder()
                .expireAfterWrite(timeout, unit)
                .maximumSize(size)
                .softValues()
                .recordStats()
                .build( new LoginFetcher());

        _time = timeout;
        _unit = unit;
        _size = size;
    }

    @Override
    public LoginReply login(Subject subject) throws CacheException {
        try {
            return _loginCache.get(subject).checkedGet();
        } catch (ExecutionException e) {
            Throwables.propagateIfPossible(e.getCause(), CacheException.class);
            throw new RuntimeException(e.getCause());
        } catch (UncheckedExecutionException e) {
            Throwables.propagateIfPossible(e.getCause());
            throw new RuntimeException(e.getCause());
        }
    }

    @Override
    public Principal map(Principal principal) throws CacheException {
        try {
            return _forwardCache.get(principal).checkedGet();
        } catch (ExecutionException e) {
            Throwables.propagateIfPossible(e.getCause(), CacheException.class);
            throw new RuntimeException(e.getCause());
        } catch (UncheckedExecutionException e) {
            Throwables.propagateIfPossible(e.getCause());
            throw new RuntimeException(e.getCause());
        }
    }

    @Override
    public Set<Principal> reverseMap(Principal principal) throws CacheException {
        try {
            return _reverseCache.get(principal).checkedGet();
        } catch (ExecutionException e) {
            Throwables.propagateIfPossible(e.getCause(), CacheException.class);
            throw new RuntimeException(e.getCause());
        } catch (UncheckedExecutionException e) {
            Throwables.propagateIfPossible(e.getCause());
            throw new RuntimeException(e.getCause());
        }
    }

    private class ForwardFetcher extends CacheLoader<Principal, CheckedFuture<Principal, CacheException>> {

        @Override
        public CheckedFuture<Principal, CacheException> load(Principal f) throws TimeoutCacheException
        {
            try {
                Principal p = _inner.map(f);
                return Futures.immediateCheckedFuture(p);
            } catch (TimeoutCacheException e) {
                throw e;
            } catch (CacheException e) {
                return Futures.immediateFailedCheckedFuture(e);
            }
        }
    }

    private class ReverseFetcher extends CacheLoader<Principal, CheckedFuture<Set<Principal>, CacheException>> {

        @Override
        public CheckedFuture<Set<Principal>, CacheException> load(Principal f) throws TimeoutCacheException
        {
            try {
                Set<Principal> s = _inner.reverseMap(f);
                return  Futures.immediateCheckedFuture(s);
            } catch (TimeoutCacheException e) {
                throw e;
            } catch (CacheException e) {
                return Futures.immediateFailedCheckedFuture(e);
            }
        }
    }

    private class LoginFetcher extends CacheLoader<Subject, CheckedFuture<LoginReply, CacheException>> {

        @Override
        public CheckedFuture<LoginReply, CacheException> load(Subject f) throws TimeoutCacheException
        {
            try {
                LoginReply s = _inner.login(f);
                return Futures.immediateCheckedFuture(s);
            } catch (TimeoutCacheException e) {
                throw e;
            } catch (CacheException e) {
                return Futures.immediateFailedCheckedFuture(e);
            }
        }
    }

    public static final String hh_login_clear_cache = " # clear cached result of login and identity mapping oprations";
    public String ac_login_clear_cache(Args args) {

        _forwardCache.invalidateAll();
        _loginCache.invalidateAll();
        _reverseCache.invalidateAll();
        return "";
    }

    public static final String hh_login_dump_cache = " # dump cached result of login and identity mapping oprations";
    public String ac_login_dump_cache(Args args) {

        StringBuilder sb = new StringBuilder();
        sb.append("Max Cache size: ").append(_size).append("\n");
        sb.append("Max Cache time: ").append(_time).append(" ")
                .append(_unit.name().toLowerCase()).append("\n");
        sb.append("Login:\n");
        for (Subject s : _loginCache.asMap().keySet()) {
            try {
                CheckedFuture<LoginReply, CacheException> out = _loginCache.getIfPresent(s);
                if (out != null) {
                    sb.append("   ").append(s.getPrincipals()).append(" => ");
                    sb.append(out.checkedGet()).append('\n');
                }
            } catch (CacheException e) {
                sb.append(e.toString()).append('\n');
            }
        }
        sb.append("Map:\n");
        for (Principal p : _forwardCache.asMap().keySet()) {
            try {
                CheckedFuture<Principal, CacheException> out = _forwardCache.getIfPresent(p);
                if (out != null) {
                    sb.append("   ").append(p).append(" => ");
                    sb.append(out.checkedGet()).append('\n');
                }
            } catch (CacheException e) {
                sb.append(e.toString()).append('\n');
            }
        }
        sb.append("ReverseMap:\n");
        for (Principal p : _reverseCache.asMap().keySet()) {
            try {
                CheckedFuture<Set<Principal>, CacheException> out = _reverseCache.getIfPresent(p);
                if (out != null) {
                    sb.append("   ").append(p).append(" => ");
                    sb.append(out.checkedGet()).append('\n');
                }
            } catch (CacheException e) {
                sb.append(e.toString()).append('\n');
            }
        }
        return sb.toString();
    }

    @Override
    public void getInfo(PrintWriter pw)
    {
        pw.append("gPlazma login cache: ").println(_loginCache.stats());
        pw.append("gPlazma map cache: ").println(_forwardCache.stats());
        pw.append("gPlazma reverse map cache: ").println(_reverseCache.stats());
    }

    @Override
    public CellInfo getCellInfo(CellInfo info)
    {
        return info;
    }
}
