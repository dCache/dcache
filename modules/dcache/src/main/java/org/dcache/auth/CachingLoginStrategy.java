package org.dcache.auth;

import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.UncheckedExecutionException;

import javax.security.auth.Subject;

import java.io.PrintWriter;
import java.security.Principal;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import diskCacheV111.util.CacheException;

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

    private final LoadingCache<Principal, Principal> _forwardCache;
    private final LoadingCache<Principal, Set<Principal>> _reverseCache;
    private final LoadingCache<Subject, LoginReply> _loginCache;

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
            return _loginCache.get(subject);
        } catch (ExecutionException e) {
            Throwables.propagateIfPossible(e.getCause(), CacheException.class);

            throw new RuntimeException(e.getCause());
        } catch (UncheckedExecutionException e) {
            Throwables.throwIfUnchecked(e.getCause());
            throw new RuntimeException(e.getCause());
        }
    }

    @Override
    public Principal map(Principal principal) throws CacheException {
        try {
            return _forwardCache.get(principal);
        } catch (ExecutionException e) {
            Throwables.propagateIfPossible(e.getCause(), CacheException.class);
            throw new RuntimeException(e.getCause());
        } catch (UncheckedExecutionException e) {
            Throwables.throwIfUnchecked(e.getCause());
            throw new RuntimeException(e.getCause());
        }
    }

    @Override
    public Set<Principal> reverseMap(Principal principal) throws CacheException {
        try {
            return _reverseCache.get(principal);
        } catch (ExecutionException e) {
            Throwables.propagateIfPossible(e.getCause(), CacheException.class);
            throw new RuntimeException(e.getCause());
        } catch (UncheckedExecutionException e) {
            Throwables.throwIfUnchecked(e.getCause());
            throw new RuntimeException(e.getCause());
        }
    }

    private class ForwardFetcher extends CacheLoader<Principal, Principal> {

        @Override
        public Principal load(Principal f) throws Exception
        {
                return _inner.map(f);
        }
    }

    private class ReverseFetcher extends CacheLoader<Principal, Set<Principal>> {

        @Override
        public Set<Principal> load(Principal f) throws Exception
        {
                return _inner.reverseMap(f);
        }
    }

    private class LoginFetcher extends CacheLoader<Subject, LoginReply> {

        @Override
        public LoginReply load(Subject f) throws Exception
        {

                return _inner.login(f);
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
            LoginReply out = _loginCache.getIfPresent(s);
            if (out != null) {
                sb.append("   ").append(s.getPrincipals()).append(" => ");
                sb.append(out).append('\n');
            }
        }
        sb.append("Map:\n");
        for (Principal p : _forwardCache.asMap().keySet()) {
            Principal out = _forwardCache.getIfPresent(p);
            if (out != null) {
                sb.append("   ").append(p).append(" => ");
                sb.append(out).append('\n');
            }
        }
        sb.append("ReverseMap:\n");
        for (Principal p : _reverseCache.asMap().keySet()) {
            Set<Principal> out = _reverseCache.getIfPresent(p);
            if (out != null) {
                sb.append("   ").append(p).append(" => ");
                sb.append(out).append('\n');
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
