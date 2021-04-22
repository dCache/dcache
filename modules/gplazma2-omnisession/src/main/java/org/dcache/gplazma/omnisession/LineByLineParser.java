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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;

import org.dcache.gplazma.omnisession.LineBasedParser.UnrecoverableParsingException;

/**
 * This class parses the contents of a file.  It does this by assuming the
 * file is written in UTF-8 and may be split into separate lines that
 * can be parsed where each line is presented one after the other.
 * <p>
 * The actual parsing of the lines is handled by another object: some
 * instance of LineBasedParser.
 * @param <T> The type of state represented the parsed file.
 */
public class LineByLineParser<T> implements Function<Path,Optional<T>>
{
    private static final Logger LOGGER = LoggerFactory.getLogger(LineByLineParser.class);

    private final Supplier<LineBasedParser<T>> parserFactory;

    public LineByLineParser(Supplier<LineBasedParser<T>> parserFactory)
    {
        this.parserFactory = parserFactory;
    }

    @Override
    public Optional<T> apply(Path file)
    {
        LineBasedParser<T> parser = parserFactory.get();

        int lineNumber = 1;
        try {
            for (String line : Files.readAllLines(file)) {
                parser.accept(line);
                lineNumber++;
            }
        } catch (IOException e) {
            LOGGER.warn("{}: {}", file, e.toString());
            return Optional.empty();
        } catch (UnrecoverableParsingException e) {
            LOGGER.warn("{}:{} {}", file, lineNumber, e.getMessage());
            return Optional.empty();
        }

        return Optional.of(parser.build());
    }
}
