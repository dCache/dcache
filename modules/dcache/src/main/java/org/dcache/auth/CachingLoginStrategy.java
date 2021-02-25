package org.dcache.auth;

import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.CheckedFuture;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.UncheckedExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;

import java.io.PrintWriter;
import java.security.Principal;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

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
    private static final Logger LOGGER = LoggerFactory.getLogger(CachingLoginStrategy.class);

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
        LOGGER.debug("Looking up login for {} in cache.", subject);
        try {
            LoginReply reply = _loginCache.get(subject).checkedGet();
            LOGGER.debug("Lookup successful for {}: {}", subject, reply);
            return reply;
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            LOGGER.debug("Looking for {} failed: {}", subject, cause.toString());
            Throwables.propagateIfPossible(cause, CacheException.class);
            throw new RuntimeException(cause);
        } catch (UncheckedExecutionException e) {
            Throwable cause = e.getCause();
            LOGGER.debug("Looking for {} failed: {}", subject, cause.toString());
            Throwables.throwIfUnchecked(cause);
            throw new RuntimeException(cause);
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
            Throwables.throwIfUnchecked(e.getCause());
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
            Throwables.throwIfUnchecked(e.getCause());
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
            LOGGER.debug("Fetching login result for {}", f);
            try {
                LoginReply s = _inner.login(f);
                LOGGER.debug("Login successful {}", s);
                return Futures.immediateCheckedFuture(s);
            } catch (TimeoutCacheException e) {
                LOGGER.debug("Login timed out");
                throw e;
            } catch (CacheException e) {
                LOGGER.debug("Login failed: {}", e.getMessage());
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
        for (Map.Entry<Subject,CheckedFuture<LoginReply, CacheException>> entry : _loginCache.asMap().entrySet()) {
            Subject s = entry.getKey();
            sb.append("   ").append(s).append(' ');
            append(sb, s);
            sb.append(" => ");
            try {
                sb.append(entry.getValue().checkedGet());
            } catch (CacheException e) {
                sb.append(e.toString());
            }
            sb.append('\n');
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

    private void append(StringBuilder sb, Subject subject)
    {
        boolean haveAddedOutput = false;

        Set<Object> publicCredentials = subject.getPublicCredentials();
        if (!publicCredentials.isEmpty()) {
            CharSequence details =  publicCredentials.stream()
                    .map(Object::toString)
                    .collect(Collectors.joining(", ", "{", "}"));
            sb.append("public=").append(details);
            haveAddedOutput = true;
        }

        Set<Object> privateCredentials = subject.getPrivateCredentials();
        if (!privateCredentials.isEmpty()) {
            if (haveAddedOutput) {
                sb.append(" ");
            }
            CharSequence details = privateCredentials.stream()
                    .map(Object::toString)
                    .collect(Collectors.joining(", ", "{", "}"));
            sb.append("private=").append(details);
            haveAddedOutput = true;
        }

        Set<Principal> principals = subject.getPrincipals();
        if (!principals.isEmpty()) {
            if (haveAddedOutput) {
                sb.append(" ");
            }
            CharSequence details = principals.stream()
                    .map(Principal::toString)
                    .collect(Collectors.joining(", ", "{", "}"));
            sb.append("principals=").append(details);
        }
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
