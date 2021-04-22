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

import static java.util.Objects.requireNonNull;

/**
 * A parser that can understand a configuration file's contents when the lines
 * from that configuration file are presented one at a time.
 * @param <T> The class that represents the file's content.
 */
public interface LineBasedParser<T>
{
    /**
     * Problem has been detected when parsing a line of text that invalidates
     * the whole model.  The message should describe the problem without
     * including the line number.
     */
    public class UnrecoverableParsingException extends Exception
    {
        public UnrecoverableParsingException(String message)
        {
            super(requireNonNull(message));
        }
    }

    /**
     * Accept a new configuration line.  The lines of a file are presented
     * one after the other until either there are no more lines or this method
     * throws an exception.
     * @param line the text of a line, without any new-line character.
     * @throws UnrecoverableParsingException the line contains an error that
     * invalidates the entire configuration file.
     */
    void accept(String line) throws UnrecoverableParsingException;

    /**
     * Return an object that represents the information that has just been
     * parsed.  This method should only be called if no calls to
     * {@link #accept(java.lang.String)} throws an exception.
     * The returned object should be immutable.
     * @return The representation of the parsed contents.
     */
    T build();
}
