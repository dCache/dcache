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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;

/**
 * This reader wraps a BufferedReader and extends the basic Reader class
 * so that it compensates for Configuration.read behaviour.  The parser's
 * behaviour results in unreliable line numbers being reported if
 * LineNumberReader is used directly.  This is due to two reasons:
 * <p>
 * First, the load method uses an internal buffer to read as much as
 * possible from the reader.  It is very likely that this will include
 * many lines, advancing the LineNumberReader so the line number count
 * will be unreliable.  The put method, when reporting a problem, will
 * very likely use a line number greater than that of the line where the
 * problem is located.
 * <p>
 * Second, when finished parsing a line, if the parsing has exhausted
 * the available data then the parser will always fetch more data.  This
 * is needed if the line ends with a backslash ('\'), but the parser does
 * this unconditionally if the buffer is exhausted.  This behaviour
 * results in an out-by-one error in the line numbers, except when reading
 * the last line.
 * <p>
 * To counter the first problem, this class replies with exactly one line
 * for each read request.  For the second problem, this class injects
 * a empty line in between each real line-read, provided the previous
 * line didn't end with a backslash.  These empty lines do not cause the
 * line number to increase but prevent the out-by-one error.
 * <p>
 * NB. In case it isn't obvious: this class is nothing more than an ugly
 * hack.  The correct solution is to write a replacement parser.
 */
public class ConfigurationParserAwareReader extends Reader
{
    private final BufferedReader _inner;
    private boolean _shouldInjectBlankLine;
    private String _remaining = "";

    public ConfigurationParserAwareReader(BufferedReader reader)
    {
        _inner = reader;
    }

    @Override
    public int read(char[] cbuf, int off, int len) throws IOException
    {
        String data = getDataForParser();
        if(data == null) {
            return -1;
        }

        int count = Math.min(len, data.length());
        System.arraycopy(data.toCharArray(), 0, cbuf, off, count);

        _remaining = data.substring(count);

        if(_remaining.isEmpty()) {
            if (_shouldInjectBlankLine){
                _shouldInjectBlankLine = false;
            } else {
                _shouldInjectBlankLine = !data.endsWith("\\\n");
            }
        }

        return count;
    }

    private String getDataForParser() throws IOException
    {
        if( !_remaining.isEmpty()) {
            return _remaining;
        }

        if(_shouldInjectBlankLine) {
            return "\n";
        }

        String data = _inner.readLine();

        return data == null ? null : data + "\n";
    }

    @Override
    public void close() throws IOException {
        _inner.close();
    }
}
