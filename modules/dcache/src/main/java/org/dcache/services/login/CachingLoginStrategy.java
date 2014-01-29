package org.dcache.services.login;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.CheckedFuture;
import com.google.common.util.concurrent.Futures;

import javax.security.auth.Subject;

import java.security.Principal;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import diskCacheV111.util.CacheException;

import dmg.cells.nucleus.CellCommandListener;

import org.dcache.auth.LoginReply;
import org.dcache.auth.LoginStrategy;
import org.dcache.util.Args;

/**
 * Caching implementation of {@link LoginStrategy}.
 */
public class CachingLoginStrategy implements LoginStrategy, CellCommandListener {

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
                .build( new ForwardFetcher());

        _reverseCache = CacheBuilder.newBuilder()
                .expireAfterWrite(timeout, unit)
                .maximumSize(size)
                .softValues()
                .build( new ReverseFetcher());

        _loginCache = CacheBuilder.newBuilder()
                .expireAfterWrite(timeout, unit)
                .maximumSize(size)
                .softValues()
                .build( new LoginFetcher());

        _time = timeout;
        _unit = unit;
        _size = size;
    }

    @Override
    public LoginReply login(Subject subject) throws CacheException {
        return _loginCache.getUnchecked(subject).checkedGet();
    }

    @Override
    public Principal map(Principal principal) throws CacheException {
        return _forwardCache.getUnchecked(principal).checkedGet();
    }

    @Override
    public Set<Principal> reverseMap(Principal principal) throws CacheException {
        return _reverseCache.getUnchecked(principal).checkedGet();
    }

    private class ForwardFetcher extends CacheLoader<Principal, CheckedFuture<Principal, CacheException>> {

        @Override
        public CheckedFuture<Principal, CacheException> load(Principal f) {
            try {
                Principal p = _inner.map(f);
                return Futures.immediateCheckedFuture(p);
            } catch (CacheException e) {
                return Futures.immediateFailedCheckedFuture(e);
            }
        }
    }

    private class ReverseFetcher extends CacheLoader<Principal, CheckedFuture<Set<Principal>, CacheException>> {

        @Override
        public CheckedFuture<Set<Principal>, CacheException> load(Principal f) {
            try {
                Set<Principal> s = _inner.reverseMap(f);
                return  Futures.immediateCheckedFuture(s);
            } catch (CacheException e) {
                return Futures.immediateFailedCheckedFuture(e);
            }
        }
    }

    private class LoginFetcher extends CacheLoader<Subject, CheckedFuture<LoginReply, CacheException>> {

        @Override
        public CheckedFuture<LoginReply, CacheException> load(Subject f) {
            try {
                LoginReply s = _inner.login(f);
                return Futures.immediateCheckedFuture(s);
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
}
