package org.dcache.services.login;

import com.google.common.base.Function;
import com.google.common.collect.MapMaker;
import com.google.common.util.concurrent.CheckedFuture;
import com.google.common.util.concurrent.Futures;
import diskCacheV111.util.CacheException;
import java.security.Principal;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import javax.security.auth.Subject;
import org.dcache.auth.LoginReply;
import org.dcache.auth.LoginStrategy;

/**
 * Caching implementation of {@link LoginStrategy}.
 */
public class CachingLoginStrategy implements LoginStrategy {

    private final LoginStrategy _inner;

    private final ConcurrentMap<Principal,CheckedFuture<Principal, CacheException>> _forwardMap;
    private final ConcurrentMap<Principal,CheckedFuture<Set<Principal>, CacheException>> _reverseMap;
    private final ConcurrentMap<Subject, CheckedFuture<LoginReply, CacheException>> _login;

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

        _forwardMap = new MapMaker()
                .expireAfterWrite(timeout, unit)
                .maximumSize(size)
                .softValues()
                .makeComputingMap( new ForwardFetcher());

        _reverseMap = new MapMaker()
                .expireAfterWrite(timeout, unit)
                .maximumSize(size)
                .softValues()
                .makeComputingMap( new ReverseFetcher());

        _login = new MapMaker()
                .expireAfterWrite(timeout, unit)
                .maximumSize(size)
                .softValues()
                .makeComputingMap( new LoginFetcher());
    }

    @Override
    public LoginReply login(Subject subject) throws CacheException {
        return _login.get(subject).checkedGet();
    }

    @Override
    public Principal map(Principal principal) throws CacheException {
        return _forwardMap.get(principal).checkedGet();
    }

    @Override
    public Set<Principal> reverseMap(Principal principal) throws CacheException {
        return _reverseMap.get(principal).checkedGet();
    }

    private class ForwardFetcher implements Function<Principal, CheckedFuture<Principal, CacheException>> {

        @Override
        public CheckedFuture<Principal, CacheException> apply(Principal f) {
            try {
                Principal p = _inner.map(f);
                return Futures.immediateCheckedFuture(p);
            } catch (CacheException e) {
                return Futures.immediateFailedCheckedFuture(e);
            }
        }
    }

    private class ReverseFetcher implements Function<Principal, CheckedFuture<Set<Principal>, CacheException>> {

        @Override
        public CheckedFuture<Set<Principal>, CacheException> apply(Principal f) {
            try {
                Set<Principal> s = _inner.reverseMap(f);
                return  Futures.immediateCheckedFuture(s);
            } catch (CacheException e) {
                return Futures.immediateFailedCheckedFuture(e);
            }
        }
    }

    private class LoginFetcher implements Function<Subject, CheckedFuture<LoginReply, CacheException>> {

        @Override
        public CheckedFuture<LoginReply, CacheException> apply(Subject f) {
            try {
                LoginReply s = _inner.login(f);
                return Futures.immediateCheckedFuture(s);
            } catch (CacheException e) {
                return Futures.immediateFailedCheckedFuture(e);
            }
        }
    }
}
