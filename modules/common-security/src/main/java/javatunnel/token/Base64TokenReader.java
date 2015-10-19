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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Base64;

public class Base64TokenReader implements TokenReader
{
    private final BufferedReader in;

    public Base64TokenReader(InputStream in)
    {
        this.in = new BufferedReader(new InputStreamReader(in));
    }

    @Override
    public byte[] readToken() throws IOException
    {
        String s = in.readLine();
        if (s == null) {
            return null;
        }
        if (!s.startsWith("enc ")) {
            throw new IOException("Invalid framing: " + s);
        }
        return Base64.getDecoder().decode(s.substring(4));
    }

    @Override
    public void close() throws IOException
    {
        in.close();
    }
}
