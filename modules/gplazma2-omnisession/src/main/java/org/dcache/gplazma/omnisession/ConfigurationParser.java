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

import com.google.common.base.Splitter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import diskCacheV111.util.FsPath;

import org.dcache.auth.attributes.HomeDirectory;
import org.dcache.auth.attributes.LoginAttribute;
import org.dcache.auth.attributes.MaxUploadSize;
import org.dcache.auth.attributes.PrefixRestriction;
import org.dcache.auth.attributes.Restrictions;
import org.dcache.auth.attributes.RootDirectory;
import org.dcache.gplazma.omnisession.ParsedConfiguration.ParsedLine;
import org.dcache.gplazma.omnisession.PrincipalPredicates.PredicateParserException;
import org.dcache.util.ByteSizeParser;
import org.dcache.util.ByteUnits;

import static java.util.Collections.emptyList;
import static org.dcache.util.ByteSizeParser.UnitPresence.OPTIONAL;
import static org.dcache.util.ByteSizeParser.Whitespace.NOT_ALLOWED;
import static org.dcache.util.Exceptions.genericCheck;

/**
 * A parser that reads the omnisession plugin's configuration file.
 */
public class ConfigurationParser implements LineBasedParser<Configuration>
{
    private static final Logger LOGGER = LoggerFactory.getLogger(ConfigurationParser.class);

    private static final Set<String> PATH_ATTRIBUTES = Set.of("home", "root", "path");
    private static final ByteSizeParser.Builder SIZE_PARSER = ByteSizeParser.using(ByteUnits.isoSymbol())
            .withWhitespace(NOT_ALLOWED)
            .withUnits(OPTIONAL);

    /** Something is wrong when parsing this line. */
    private static class BadLineException extends Exception
    {
        BadLineException(String message)
        {
            super(message);
        }
    }

    private Optional<List<LoginAttribute>> defaultAttributes = Optional.empty();
    private final List<ParsedLine> targetedAttributes = new ArrayList<>();

    private int lineNumber;
    private boolean badConfigFile;

    public static void checkBadLine(boolean isLineOk, String message,
            Object... args) throws BadLineException
    {
        genericCheck(isLineOk, BadLineException::new, message, args);
    }

    @Override
    public void accept(String rawLine) throws UnrecoverableParsingException
    {
        lineNumber++;

        String line = rawLine.trim();
        if (line.isEmpty() || line.startsWith("#")) {
            return;
        }

        try {
            if (line.startsWith("DEFAULT ")) {
                checkBadLine(defaultAttributes.isEmpty(), "\"DEFAULT\" is already defined.");
                List<LoginAttribute> attributes = parseAttributes(line.substring(8));
                defaultAttributes = Optional.of(attributes);
            } else {
                PrincipalPredicates.ParsedLine result;
                try {
                    result = PrincipalPredicates.parseFirstPredicate(line);
                } catch (PredicateParserException e) {
                    throw new BadLineException("Bad predicate: " + e.getMessage());
                }

                ParsedLine parsedLine;
                try {
                    var attributes = parseAttributes(result.remaining());
                    parsedLine = ParsedLine.success(lineNumber, result.predicate(),
                            attributes);
                } catch (BadLineException e) {
                    LOGGER.warn("Bad attributes in line {}: {}", lineNumber, e.getMessage());
                    parsedLine = ParsedLine.failure(lineNumber, result.predicate(),
                            e.getMessage());
                }
                targetedAttributes.add(parsedLine);
            }
        } catch (BadLineException e) {
            badConfigFile = true;
            throw new UnrecoverableParsingException(e.getMessage());
        }
    }

    private List<LoginAttribute> parseAttributes(String description) throws BadLineException
    {
        List<LoginAttribute> attributes = new ArrayList<>();

        boolean isReadOnly = false;
        Set<Class<? extends LoginAttribute>> addedAttributes = new HashSet<>();

        for (String attr : Splitter.on(' ').omitEmptyStrings().split(description)) {
            try {
                if (attr.equals("read-only")) {
                    checkBadLine(!isReadOnly, "already defined 'read-only'");
                    isReadOnly = true;
                    attributes.add(Restrictions.readOnly());
                    continue;
                }

                int idx = attr.indexOf(':');

                checkBadLine(idx > -1, "Missing ':'");
                checkBadLine(idx != 0, "Missing type");
                checkBadLine(idx < attr.length()-1, "Missing argument");

                String type = attr.substring(0, idx);
                String arg = attr.substring(idx+1);

                if (PATH_ATTRIBUTES.contains(type)) {
                    checkBadLine(arg.startsWith("/"), "Argument must be an absolute"
                            + " path");
                }

                LoginAttribute attribute;
                switch (type) {
                case "root":
                    attribute = new RootDirectory(arg);
                    break;

                case "home":
                    attribute = new HomeDirectory(arg);
                    break;

                case "prefix":
                    attribute = new PrefixRestriction(FsPath.create(arg));
                    break;

                case "max-upload":
                    try {
                        attribute = new MaxUploadSize(SIZE_PARSER.parse(arg));
                    } catch (NumberFormatException e) {
                        throw new BadLineException("Bad file size: " + e.getMessage());
                    }
                    break;

                default:
                    throw new BadLineException("Unknown type \"" + type + "\"");
                }

                if (!addedAttributes.add(attribute.getClass())) {
                    throw new BadLineException("Multiple " + type + " defined.");
                }

                attributes.add(attribute);
            } catch (BadLineException e) {
                throw new BadLineException("Bad attribute \"" + attr + "\": " + e.getMessage());
            }
        }

        return attributes;
    }

    @Override
    public Configuration build()
    {
        if (badConfigFile) {
            throw new RuntimeException("Cannot create configuration from bad file.");
        }

        List<LoginAttribute> defaults = defaultAttributes.orElse(emptyList());
        return new ParsedConfiguration(defaults, targetedAttributes);
    }
}
