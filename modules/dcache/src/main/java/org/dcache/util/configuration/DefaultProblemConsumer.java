/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2011 - 2018 Deutsches Elektronen-Synchrotron
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
package org.dcache.util.configuration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.LineNumberReader;

/**
 * This class provides the default behaviour if no problem
 * consumer is registered: warnings are logged and errors
 * result in an IllegalArgumentException being thrown.
 */
public class DefaultProblemConsumer implements ProblemConsumer
{
    private static final Logger LOGGER =
        LoggerFactory.getLogger(DefaultProblemConsumer.class);

    private String _filename;
    private LineNumberReader _reader;

    protected String addContextTo(String message)
    {
        if( _filename == null || _reader == null) {
            return message;
        }

        return _filename + ":" + _reader.getLineNumber() + ": " + message;
    }

    @Override
    public void error(String message)
    {
        throw new IllegalArgumentException(addContextTo(message));
    }

    @Override
    public void warning(String message)
    {
        LOGGER.warn(addContextTo(message));
    }

    @Override
    public void info(String message)
    {
        LOGGER.info(addContextTo(message));
    }

    @Override
    public void setFilename(String name)
    {
        _filename = name;
    }

    @Override
    public void setLineNumberReader(LineNumberReader reader)
    {
        _reader = reader;
    }
}
