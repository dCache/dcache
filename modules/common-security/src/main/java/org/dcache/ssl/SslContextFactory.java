/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2015 Deutsches Elektronen-Synchrotron
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
package org.dcache.ssl;

import eu.emi.security.authn.x509.X509Credential;
import java.security.GeneralSecurityException;
import javax.annotation.Nullable;

/**
 * Factory for creating SSL context instances. (e.g., javax.net.SSLContext or
 * io.netty.handler.ssl.SslContext).
 */
public interface SslContextFactory {

    /**
     * Provides an SslContext that will use the supplied optional client credential for
     * authentication.
     *
     * @param type       of the context object to create.
     * @param credential the credential to use, or null if no X.509 credential.
     * @return an SSL context to use.
     * @throws GeneralSecurityException if there is a problem establishing the context.
     */
    <T> T getContext(Class<T> type, @Nullable X509Credential credential)
          throws GeneralSecurityException;
}
