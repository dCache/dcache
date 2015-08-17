/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2015 Deutsches Elektronen-Synchrotron
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
package javatunnel.token;

import javatunnel.dss.DssContext;

import java.io.IOException;
import java.io.OutputStream;

public class WrappingOutputStream extends OutputStream
{
    private static final int ARRAYMAXLEN = 4096;

    private final TokenWriter out;
    private final DssContext context;
    private final byte[] buffer = new byte[ARRAYMAXLEN];
    private int pos;

    public WrappingOutputStream(TokenWriter out, DssContext context)
    {
        this.out = out;
        this.context = context;
    }

    @Override
    public void write(int b) throws IOException
    {
        buffer[pos] = (byte)b;
        ++pos;
        if (pos >= ARRAYMAXLEN) {
            flush();
        }
    }

    @Override
    public void flush() throws IOException
    {
        out.write(context.wrap(buffer, 0, pos));
        pos = 0;
    }

    @Override
    public void close() throws IOException
    {
        out.close();
    }
}
