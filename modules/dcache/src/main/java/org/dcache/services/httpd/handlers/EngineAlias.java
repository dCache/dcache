/*
 * dCache - http://www.dcache.org/
 *
 * Copyright (C) 2026 Deutsches Elektronen-Synchrotron
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
package org.dcache.services.httpd.handlers;

import static java.util.Objects.requireNonNull;

import dmg.util.HttpException;
import dmg.util.HttpRequest;
import dmg.util.HttpResponseEngine;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import org.dcache.services.httpd.util.StandardHttpRequest;

/**
 * Answers an alias with an {@link HttpResponseEngine}.
 *
 * @author arossi
 */
public class EngineAlias implements AliasHandler {

    private final HttpResponseEngine engine;

    public EngineAlias(HttpResponseEngine engine) {
        this.engine = requireNonNull(engine);
    }

    @Override
    public void handle(HttpServletRequest request, HttpServletResponse response)
          throws IOException, HttpException {
        HttpRequest proxy = StandardHttpRequest.of(request, response);
        engine.queryUrl(proxy);
        proxy.getPrintWriter().flush();
    }
}
