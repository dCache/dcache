/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2017 Deutsches Elektronen-Synchrotron
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
package org.dcache.macaroons;

import com.github.nitram509.jmacaroons.GeneralCaveatVerifier;

import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.Optional;

import static org.dcache.macaroons.CaveatValues.*;

/**
 * Extract context information from caveats.  Fails if those caveats are
 * somehow invalid.
 */
public class ContextExtractingCaveatVerifier implements GeneralCaveatVerifier
{
    private final MacaroonContext context = new MacaroonContext();

    private String error;

    @Override
    public boolean verifyCaveat(String serialised)
    {
        try {
            Caveat caveat = new Caveat(serialised);
            String value = caveat.getValue();

            switch (caveat.getType()) {
            case HOME:
                context.updateHome(value);
                return true;

            case ROOT:
                context.updateRoot(value);
                return true;

            case PATH:
                context.updatePath(value);
                return true;

            case IDENTITY:
                parseIdentityCaveatValue(context, value);
                return true;

            case ACTIVITY:
                context.updateAllowedActivities(parseActivityCaveatValue(value));
                return true;

            case BEFORE:
                try {
                    Instant expiry = Instant.parse(value);
                    if (Instant.now().isAfter(expiry)) {
                        throw new InvalidCaveatException("expired");
                    }
                    context.updateExpiry(expiry);
                    return true;
                } catch (DateTimeParseException e) {
                    throw InvalidCaveatException.wrap("Bad ISO 8601 timestamp", e);
                }

            case MAX_UPLOAD:
                try {
                    long maxUpload = Long.parseLong(value);
                    context.updateMaxUpload(maxUpload);
                    return true;
                } catch (NumberFormatException e) {
                    throw InvalidCaveatException.wrap("Bad " + CaveatType.MAX_UPLOAD.getLabel(), e);
                }
            }
        } catch (InvalidCaveatException e) {
            error = e.getMessage() + ": " + serialised;
        }

        return false;
    }

    public MacaroonContext getContext()
    {
        return context;
    }

    public Optional<String> getError()
    {
        return Optional.ofNullable(error);
    }
}

