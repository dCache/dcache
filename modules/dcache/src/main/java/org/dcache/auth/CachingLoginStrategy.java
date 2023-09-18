package org.dcache.auth;

import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.UncheckedExecutionException;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.TimeoutCacheException;
import dmg.cells.nucleus.CellCommandListener;
import dmg.cells.nucleus.CellInfo;
import dmg.cells.nucleus.CellInfoProvider;
import java.io.PrintWriter;
import java.security.Principal;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import javax.security.auth.Subject;
import org.dcache.util.Args;

/**
 * Caching implementation of {@link LoginStrategy}.
 */
public class CachingLoginStrategy implements LoginStrategy, CellCommandListener, CellInfoProvider {

    private final LoginStrategy _inner;

    private final LoadingCache<Principal, CompletableFuture<Principal>> _forwardCache;
    private final LoadingCache<Principal, CompletableFuture<Set<Principal>>> _reverseCache;
    private final LoadingCache<Subject, CompletableFuture<LoginReply>> _loginCache;

    private final long _time;
    private final TimeUnit _unit;
    private final int _size;

    /**
     * Create an instance of LoginStrategy
     *
     * @param inner   {@link LoginStrategy} used for fetching data.
     * @param size    maximal size of cached entries per cache table
     * @param timeout cache entry life time.
     * @param unit    the time unit of the timeout argument
     */
    public CachingLoginStrategy(LoginStrategy inner, int size, long timeout, TimeUnit unit) {

        _inner = inner;

        _forwardCache = Caffeine.newBuilder()
              .expireAfterWrite(timeout, unit)
              .maximumSize(size)
              .softValues()
              .recordStats()
              .build(new ForwardFetcher());

        _reverseCache = Caffeine.newBuilder()
              .expireAfterWrite(timeout, unit)
              .maximumSize(size)
              .softValues()
              .recordStats()
              .build(new ReverseFetcher());

        _loginCache = Caffeine.newBuilder()
              .expireAfterWrite(timeout, unit)
              .maximumSize(size)
              .softValues()
              .recordStats()
              .build(new LoginFetcher());

        _time = timeout;
        _unit = unit;
        _size = size;
    }

    @Override
    public LoginReply login(Subject subject) throws CacheException {
        try {
            return _loginCache.get(subject).get();
        } catch (InterruptedException e) {
            throw new TimeoutCacheException("Request interrupted");
        } catch (ExecutionException | CompletionException e) {
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
            return _forwardCache.get(principal).get();
        } catch (InterruptedException e) {
            throw new TimeoutCacheException("Request interrupted");
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
            return _reverseCache.get(principal).get();
        } catch (InterruptedException e) {
            throw new TimeoutCacheException("Request interrupted");
        } catch (ExecutionException e) {
            Throwables.propagateIfPossible(e.getCause(), CacheException.class);
            throw new RuntimeException(e.getCause());
        } catch (UncheckedExecutionException e) {
            Throwables.throwIfUnchecked(e.getCause());
            throw new RuntimeException(e.getCause());
        }
    }

    private class ForwardFetcher implements CacheLoader<Principal, CompletableFuture<Principal>> {

        @Override
        public CompletableFuture<Principal> load(Principal f) throws CacheException {
            try {
                return CompletableFuture.completedFuture(_inner.map(f));
            } catch (CacheException e) {
                Throwables.propagateIfPossible(e, TimeoutCacheException.class);
                return CompletableFuture.failedFuture(e);
            }
        }
    }

    private class ReverseFetcher implements CacheLoader<Principal, CompletableFuture<Set<Principal>>> {

        @Override
        public CompletableFuture<Set<Principal>> load(Principal f) throws CacheException {
            try {
                return CompletableFuture.completedFuture(_inner.reverseMap(f));
            } catch (CacheException e) {
                Throwables.propagateIfPossible(e, TimeoutCacheException.class);
                return CompletableFuture.failedFuture(e);
            }
        }
    }

    private class LoginFetcher implements CacheLoader<Subject, CompletableFuture<LoginReply>> {

        @Override
        public CompletableFuture<LoginReply> load(Subject f) throws CacheException {
            try {
                return CompletableFuture.completedFuture(_inner.login(f));
            } catch (CacheException e) {
                Throwables.propagateIfPossible(e, TimeoutCacheException.class);
                return CompletableFuture.failedFuture(e);
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
                CompletableFuture<LoginReply> out = _loginCache.getIfPresent(s);
                if (out != null) {
                    sb.append("   ").append(Subjects.toString(s)).append(" => ");
                    sb.append(out.get()).append('\n');
                }
            } catch (ExecutionException | InterruptedException e) {
                sb.append(Throwables.getRootCause(e)).append('\n');
            }
        }
        sb.append("Map:\n");
        for (Principal p : _forwardCache.asMap().keySet()) {
            try {
                CompletableFuture<Principal> out = _forwardCache.getIfPresent(p);
                if (out != null) {
                    sb.append("   ").append(p).append(" => ");
                    sb.append(out.get()).append('\n');
                }
            } catch (ExecutionException | InterruptedException e) {
                sb.append(Throwables.getRootCause(e)).append('\n');
            }
        }
        sb.append("ReverseMap:\n");
        for (Principal p : _reverseCache.asMap().keySet()) {
            try {
                CompletableFuture<Set<Principal>> out = _reverseCache.getIfPresent(p);
                if (out != null) {
                    sb.append("   ").append(p).append(" => ");
                    sb.append(out.get()).append('\n');
                }
            } catch (ExecutionException | InterruptedException e) {
                sb.append(Throwables.getRootCause(e)).append('\n');
            }
        }
        return sb.toString();
    }

    @Override
    public void getInfo(PrintWriter pw) {
        pw.append("gPlazma login cache: ").println(_loginCache.stats());
        pw.append("gPlazma map cache: ").println(_forwardCache.stats());
        pw.append("gPlazma reverse map cache: ").println(_reverseCache.stats());
    }

    @Override
    public CellInfo getCellInfo(CellInfo info) {
        return info;
    }
}
