/*
 * dCache - http://www.dcache.org/
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
package org.dcache.gplazma.oidc.profiles;

import com.google.common.base.Splitter;
import java.util.Map;
import org.dcache.gplazma.oidc.ProfileFactory;

/**
 * A class for creating OidcProfile instances.  The {@literal accept} option is processes as a
 * comma-separated list of keywords.  Currently supported keywords are {@literal username} and
 * {@literal groups}.  Other options are ignored.
 */
public class OidcProfileFactory implements ProfileFactory {

    @Override
    public OidcProfile create(Map<String, String> arguments) {
        boolean username = false;
        boolean groups = false;
        String acceptValue = arguments.get("accept");
        if (acceptValue != null) {
            for (String item : Splitter.on(',').split(acceptValue)) {
                switch (item) {
                    case "username":
                        username = true;
                        break;
                    case "groups":
                        groups = true;
                        break;
                    default:
                        throw new IllegalArgumentException("Unknown accept item \"" + item + "\"");
                }
            }
        }

        return new OidcProfile(username, groups);
    }
}
