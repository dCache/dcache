/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2021 - 2023 Deutsches Elektronen-Synchrotron
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

import io.netty.handler.ssl.SslContext;

/**
 * Netty SslContext context factory which uses native OpenSsl if available, but falls back to Java
 * if not.
 */
public class NettySslContextFactory extends AbstractSslContextFactory<SslContext> {

    public NettySslContextFactory() {
        super(SslContext.class);
    }
}