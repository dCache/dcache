package org.dcache.gplazma;

import junit.framework.TestCase;
import org.dcache.auth.GidPrincipal;
import org.dcache.auth.UidPrincipal;
import org.dcache.auth.UserNamePrincipal;
import org.dcache.gplazma.monitor.LoginMonitor;
import org.dcache.gplazma.monitor.LoginResult;
import org.junit.Test;

import java.util.Set;

import static java.util.concurrent.TimeUnit.SECONDS;


public class RecordFailedLoginsTest extends TestCase {

    LoginResult _resuttFirstEntry;

    // it will be evicted only if no entries are ever read (.get(), .getIfPresent()),
    // that is why the test on eviction is done on the second entry
    LoginResult _resuttSecondtEntry;

    LoginResult _result;

    @Test
    public void testCacheDelete() {

        // fill with random principals
        RecordFailedLogins observer = new RecordFailedLogins();
        observer.setLoginFailureCacheSize(20);
        observer.setLoginFailureCacheSizeExpiry(60);
        observer.setLoginFailureCacheSizeExpiryUnit(SECONDS);
        observer.initialize();

        _resuttFirstEntry = new LoginResult();
        _resuttFirstEntry.setValidationResult(LoginMonitor.Result.FAIL);
        _resuttFirstEntry.setInitialPrincipals(Set.of(
                new UserNamePrincipal("testuser" + 0),
                new UidPrincipal("1000" + 0),
                new GidPrincipal("1000", true)));

        _resuttSecondtEntry = new LoginResult();
        _resuttSecondtEntry.setValidationResult(LoginMonitor.Result.FAIL);
        _resuttSecondtEntry.setInitialPrincipals(Set.of(
                new UserNamePrincipal("testuser" + 1),
                new UidPrincipal("1000" + 1),
                new GidPrincipal("1000", true)));

        observer.accept(_resuttFirstEntry);

        for (int i = 2; i < 55; i++) {

            _result = new LoginResult();
            _result.setValidationResult(LoginMonitor.Result.FAIL);
            _result.setInitialPrincipals(Set.of(
                    new UserNamePrincipal("testuser" + i),
                    new UidPrincipal("1000" + i),
                    new GidPrincipal("1000", true)));

            observer.accept(_result);

            if (i == 10) {
                boolean hasFirstEntry = (observer.has(_resuttFirstEntry));
                assertTrue("First inserted subject still exists", hasFirstEntry);
            }
        }
        // check if second entry is still there
        boolean hasSecondEntryAfter = (observer.has(_resuttSecondtEntry));
        assertFalse("First inserted subject should have been evicted", hasSecondEntryAfter);
    }

}


