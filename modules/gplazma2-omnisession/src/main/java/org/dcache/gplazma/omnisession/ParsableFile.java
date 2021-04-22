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

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;

import static java.time.temporal.ChronoUnit.SECONDS;
import static java.util.Objects.requireNonNull;

/**
 * This object represents a some generic file that may be parsed to obtain an
 * object that represents its content.  The file is monitored for any changes
 * so that updates to the file are reflected without requiring an
 * explicit reload operation.
 * <p>
 * The processing model is as follows: the {@link #getContents()} method
 * returns a representation of the file's contents at the time the call was
 * made.  This method returns an Optional value that may contain the information
 * parsed from the file.  If there is a problem (e.g., file doesn't exist,
 * cannot be read, non-recoverable parser error) then the method returns an
 * empty Optional, otherwise the returned value contains the parsed information
 * from the file.
 * <p>
 * The object returned by {@code getContents} is immutable.  Any changes to the
 * file's content will <b>not</b> be reflected in any existing objects; however,
 * subsequent calls to {@code #getContents} will return objects that reflect the
 * updated file's new content.
 * <p>
 * The anticipated use is to call {#getContents} relatively often, with the
 * returned object being kept only for as long as the calling code requires
 * consistent information.
 * <p>
 * This class is thread-safe.
 *
 * @param <T> The type of Object that represents the file's contents.
 */
public class ParsableFile<T> implements Supplier<Optional<T>>
{
    private static final Logger LOGGER = LoggerFactory.getLogger(ParsableFile.class);

    private final Path file;
    private final Function<Path,Optional<T>> parser;

    private Instant whenStatProhibitionEnds = Instant.MIN;
    private Instant mtimeWhenFileParsed = Instant.MIN;

    private Optional<T> info = Optional.empty();

    private final Clock clock;

    @Nullable
    private String lastError;

    public ParsableFile(Function<Path,Optional<T>> parser, Path file)
    {
        this(Clock.systemUTC(), parser, file);
    }

    @VisibleForTesting
    ParsableFile(Clock clock, Function<Path,Optional<T>> parser, Path file)
    {
        this.clock = clock;
        this.parser = requireNonNull(parser);
        this.file = requireNonNull(file);
    }

    /**
     * Return an immutable object representing the file's contents at the time
     * this method is called.  If there is a problem reading the contents then
     * the returned value is empty, otherwise the return object contains
     * the parsed state of the file.
     * @return Optionally the current parsed content of the file.
     */
    @Override
    public synchronized Optional<T> get()
    {
        /* REVISIT: the current model serialises all threads, with operations
         * such as stat and parsing a file delaying any other (concurrent)
         * threads.  A future version could do better; for example, by having
         * any concurrent threads use non-empty cached values while the reload
         * is taking place.
         */

        Instant now = Instant.now(clock);

        // Avoid stat-ing the files too often: this can be (relatively) slow.
        if (now.isBefore(whenStatProhibitionEnds)) {
            return info;
        }
        whenStatProhibitionEnds = now.plus(1, SECONDS);

        Instant fileMTime;
        try {
            fileMTime = Files.getLastModifiedTime(file).toInstant();
            lastError = null; // if we see subsequent problem, log it.
        } catch (IOException e) {
            String thisError = e.toString();
            if (!thisError.equals(lastError)) {
                LOGGER.warn("Failed to stat {}: {}", file, thisError);
                lastError = thisError;  // suppress logging the same error
            }
            info = Optional.empty();
            return info;
        }

        if (fileMTime.isAfter(mtimeWhenFileParsed)) {
            mtimeWhenFileParsed = fileMTime;
            LOGGER.info("Reloading {}", file);
            info = parser.apply(file);
            info.ifPresent(Objects::requireNonNull);
        }

        return info;
    }
}
