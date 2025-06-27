/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2022 Deutsches Elektronen-Synchrotron
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.dcache.gplazma;

import java.security.Principal;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import javax.security.auth.Subject;
import org.dcache.auth.BearerTokenCredential;
import org.dcache.auth.Origin;
import org.dcache.auth.PasswordCredential;
import org.dcache.gplazma.monitor.LoginResult;
import org.dcache.gplazma.monitor.LoginResultPrinter;
import org.globus.gsi.gssapi.jaas.SimplePrincipal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A LoginResult observer that logs the first bad login attempt.
 */
public class RecordFailedLogins implements LoginObserver, ReloadObserver {

    private static final Logger LOGGER = LoggerFactory.getLogger(RecordFailedLogins.class);

    /**
     * Storage class for failed login attempts.  This allows gPlazma to refrain from filling up log
     * files should a client attempt multiple login attempts that all fail.  We must be careful
     * about how we store the incoming Subjects.
     * <p>
     * This class is thread-safe.
     */
    private static class KnownFailedLogins {

        private final Set<Subject> _failedLogins =
              new CopyOnWriteArraySet<>();

        /**
         * In general, this class does not store any private credential since doing this would be
         * against the general security advise of only storing sensitive material (e.g., passwords)
         * for as long as is necessary.
         * <p>
         * However, the class may wish to distinguish between different login attempts based
         * information contained in private credentials.  To support this, principals may be added
         * that contain non-sensitive information contained in a private credential.
         */
        private static void addPrincipalsForPrivateCredentials(Set<Principal> principals,
              Set<Object> privateCredentials) {
            for (Object credential : privateCredentials) {
                if (credential instanceof PasswordCredential) {
                    String description = ((PasswordCredential) credential).describeCredential();
                    principals.add(new SimplePrincipal(description));
                } else if (credential instanceof BearerTokenCredential) {
                    String description = ((BearerTokenCredential) credential).describeToken();
                    principals.add(new SimplePrincipal(description));
                }
            }
        }

        /**
         * Calculate the storage Subject, given an incoming subject.  The storage subject is similar
         * to the supplied Subject but has sensitive material (like passwords) removed and is
         * location agnostic (e.g., any Origin principals are removed).
         */
        private static Subject storageSubjectFor(LoginResult result) {
            Subject storage = new Subject();

            LoginResult.AuthPhaseResult authPhase = result.getAuthPhase();

            storage.getPublicCredentials().addAll(authPhase.getPublicCredentials());

            Set<Principal> principals = storage.getPrincipals();

            authPhase.getPrincipals().getBefore().stream()
                      .filter(p -> !(p instanceof Origin))
                      .forEach(principals::add);

            addPrincipalsForPrivateCredentials(principals, authPhase.getPrivateCredentials());

            return storage;
        }

        private boolean has(LoginResult result) {
            Subject storage = storageSubjectFor(result);
            return _failedLogins.contains(storage);
        }

        private void add(LoginResult result) {
            Subject storage = storageSubjectFor(result);
            _failedLogins.add(storage);
        }

        private void remove(LoginResult result) {
            Subject storage = storageSubjectFor(result);
            _failedLogins.remove(storage);
        }

        private void clear() {
            _failedLogins.clear();
        }
    }

    private final KnownFailedLogins _failedLogins = new KnownFailedLogins();

    @Override
    public void accept(LoginResult result) {
        if (result.isSuccessful()) {
            _failedLogins.remove(result);
        } else {
            if (!_failedLogins.has(result)) {
                _failedLogins.add(result);

                if (result.hasStarted()) {
                    LoginResultPrinter printer = new LoginResultPrinter(result);
                    LOGGER.warn("Login attempt failed; " +
                                "detailed explanation follows:\n{}",
                          printer.print());
                } else {
                    LOGGER.warn("Login attempt failed: {}", result.getValidationError());
                }
            }
        }
    }

    @Override
    public void configReloaded() {
        _failedLogins.clear();
    }
}
