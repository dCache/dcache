/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2021 Deutsches Elektronen-Synchrotron
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

package org.dcache.http;

import dmg.cells.nucleus.CDC;
import io.netty.handler.ssl.SslContext;
import org.dcache.ssl.CanlContextFactory;

/**
 * Netty SslContext context factory which uses native OpenSsl if available, but falls
 * back to Java if not.
 */
public class NettySslContextFactoryBean extends AbstractSslContextFactoryBean<SslContext> {

  @Override
  public SslContext getObject() throws Exception {
    return CanlContextFactory.custom()
        .withCertificateAuthorityPath(serverCaPath)
        .withCrlCheckingMode(crlCheckingMode)
        .withOcspCheckingMode(ocspCheckingMode)
        .withCertificatePath(serverCertificatePath)
        .withKeyPath(serverKeyPath)
        .withLazy(false)
        .withLoggingContext(new CDC()::restore)
        .buildWithCaching(SslContext.class)
        .call();
  }

  @Override
  public Class<?> getObjectType() {
    return SslContext.class;
  }
}