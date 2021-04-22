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
package org.dcache.gplazma.omnisession;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.Principal;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;

import org.dcache.auth.attributes.LoginAttribute;
import org.dcache.gplazma.AuthenticationException;

import static java.util.Collections.unmodifiableList;
import static java.util.List.copyOf;


/**
 * An instance of this object represents the information stored in the
 * OmniSession configuration file at some specific time.
 */
public class ParsedConfiguration implements Configuration
{
    private static final Logger LOGGER = LoggerFactory.getLogger(ParsedConfiguration.class);

    static class ParsedLine
    {
        private final Predicate<Principal> predicate;
        private final List<LoginAttribute> attributes;
        private final String error;
        private final int lineNumber;

        public static ParsedLine success(int lineNumber, Predicate<Principal> predicate, List<LoginAttribute> attributes)
        {
            return new ParsedLine(lineNumber, predicate, attributes, null);
        }

        public static ParsedLine failure(int lineNumber, Predicate<Principal> predicate, String error)
        {
            return new ParsedLine(lineNumber, predicate, null, error);
        }

        public boolean isFailure()
        {
            return error != null;
        }

        private ParsedLine(int lineNumber, Predicate<Principal> predicate, List<LoginAttribute> attributes, String error) {
            this.predicate = predicate;
            this.attributes = attributes;
            this.error = error;
            this.lineNumber = lineNumber;
        }
    }


    private final List<LoginAttribute> defaultAttributes;
    private final List<ParsedLine> configLines;

    ParsedConfiguration(List<LoginAttribute> defaultAttributes,
            List<ParsedLine> lines)
    {
        this.defaultAttributes = unmodifiableList(copyOf(defaultAttributes));
        this.configLines = unmodifiableList(copyOf(lines));
    }

    @Override
    public List<LoginAttribute> attributesFor(Set<Principal> principals)
            throws AuthenticationException
    {
        Set<Class<? extends LoginAttribute>> addedAttributes = new HashSet<>();

        List<LoginAttribute> attributesToAdd = new ArrayList<>();
        StringBuilder errorLineNumbers = new StringBuilder();
        int errorLineNumberToAdd = -1;

        for (ParsedLine line : configLines) {
            if (!principals.stream().anyMatch(p -> line.predicate.test(p))) {
                continue;
            }

            if (line.isFailure()) {
                if (errorLineNumberToAdd != -1) {
                    if (errorLineNumbers.length() != 0) {
                        errorLineNumbers.append(", ");
                    }
                    errorLineNumbers.append(errorLineNumberToAdd);
                }
                errorLineNumberToAdd = line.lineNumber;
                LOGGER.debug("Login touched bad line {}: {}", line.lineNumber,
                        line.error);
            } else {
                if (errorLineNumberToAdd == -1) {
                    for (LoginAttribute attribute : line.attributes) {
                        if (!addedAttributes.contains(attribute.getClass())) {
                            addedAttributes.add(attribute.getClass());
                            attributesToAdd.add(attribute);
                            LOGGER.debug("Adding attribute from line {}: {}",
                                    line.lineNumber, attribute);
                        } else {
                            LOGGER.debug("Skipping attribute from line {}: {}",
                                    line.lineNumber, attribute);
                        }
                    }
                }
            }
        }

        if (errorLineNumberToAdd != -1) {
            boolean moreThanOneErrorLine = errorLineNumbers.length() > 0;
            if (moreThanOneErrorLine) {
                errorLineNumbers.append(" and ");
            }

            errorLineNumbers.append(errorLineNumberToAdd);

            String msg = "Bad " + (moreThanOneErrorLine ? "lines" : "line") + ": "
                    + errorLineNumbers;
            LOGGER.debug("Aborting login: {}", msg);
            throw new AuthenticationException(msg);
        }

        for (LoginAttribute attribute : defaultAttributes) {
            if (!addedAttributes.contains(attribute.getClass())) {
                addedAttributes.add(attribute.getClass());
                attributesToAdd.add(attribute);
                LOGGER.debug("Adding default attribute {}", attribute);
            } else {
                LOGGER.debug("Skipping default attribute {}, already applied", attribute);
            }
        }

        if (attributesToAdd.isEmpty()) {
            throw new AuthenticationException("Unknown user");
        }

        return attributesToAdd;
    }
}
