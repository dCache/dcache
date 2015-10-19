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

import org.dcache.dss.DssContext;

import java.io.IOException;
import java.io.InputStream;

public class UnwrappingInputStream extends InputStream
{
    private final TokenReader in;
    private final DssContext context;
    private byte[] buffer;
    private int pos;

    public UnwrappingInputStream(TokenReader in, DssContext context)
    {
        this.in = in;
        this.context = context;
    }

    @Override
    public int read() throws IOException
    {
        if (buffer == null || pos >= buffer.length) {
            byte[] token = in.readToken();
            if (token == null) {
                return -1;
            }
            buffer = context.unwrap(token);
            pos = 0;
        }
        return (int) buffer[pos++];
    }

    @Override
    public void close() throws IOException
    {
        in.close();
    }
}
