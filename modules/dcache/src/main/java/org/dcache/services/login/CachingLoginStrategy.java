package org.dcache.services.login;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.util.concurrent.CheckedFuture;
import com.google.common.util.concurrent.Futures;
import diskCacheV111.util.CacheException;
import java.security.Principal;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import javax.security.auth.Subject;
import org.dcache.auth.LoginReply;
import org.dcache.auth.LoginStrategy;

/**
 * Caching implementation of {@link LoginStrategy}.
 */
public class CachingLoginStrategy implements LoginStrategy {

    private final LoginStrategy _inner;

    private final Cache<Principal,CheckedFuture<Principal, CacheException>> _forwardCache;
    private final Cache<Principal,CheckedFuture<Set<Principal>, CacheException>> _reverseCache;
    private final Cache<Subject, CheckedFuture<LoginReply, CacheException>> _loginCache;

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
}
