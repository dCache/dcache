/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2018 Deutsches Elektronen-Synchrotron
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
package org.dcache.util;

import java.io.PrintWriter;
import java.io.Writer;

/**
 * An implementation of PrintWriter that indents each line by some prefix.  Any
 * empty lines are left empty; i.e., there is no indent.
 */
public class LineIndentingPrintWriter extends PrintWriter
{
    private final String prefix;
    private boolean isLineStart = true;

    public LineIndentingPrintWriter(Writer inner, String prefix)
    {
        super(inner);
        this.prefix = prefix;
    }

    @Override
    public void println()
    {
        super.println();
        isLineStart = true;
    }

    @Override
    public void write(int c)
    {
        if (c == '\n') {
            isLineStart = true;
        } else {
            if (isLineStart) {
                isLineStart = false;
                super.write(prefix);
            }
        }
        super.write(c);
    }

    @Override
    public void write(char cbuf[], int off, int len)
    {
        int curr = off;
        int index = indexOf(cbuf, '\n', curr);
        while (index != -1 && index <= off+len) {
            if (isLineStart) {
                isLineStart = false;
                super.write(prefix, 0, prefix.length());
            }
            int count = 1 + index - curr; // +1 to include '\n'
            super.write(cbuf, curr, count);
            curr = index+1;
            index = indexOf(cbuf, '\n', curr);
            isLineStart = true;
        }

        if (curr < off+len) {
            if (isLineStart) {
                isLineStart = false;
                super.write(prefix, 0, prefix.length());
            }
            super.write(cbuf, curr, off+len-curr);
        }
    }

    private int indexOf(char buf[], char c, int off)
    {
        for (int i = off; i < buf.length; i++) {
            if (buf [i] == c) {
                return i;
            }
        }
        return -1;
    }

    @Override
    public void write(String str)
    {
        write(str, 0, str.length());
    }

    @Override
    public void write(String str, int off, int len)
    {
        int curr = off;
        int index = str.indexOf('\n', curr);
        while (index != -1 && index <= off+len) {
            if (isLineStart) {
                isLineStart = false;
                super.write(prefix, 0, prefix.length());
            }
            int count = 1 + index - curr; // +1 to include '\n'
            super.write(str, curr, count);
            curr = index+1;
            index = str.indexOf('\n', curr);
            isLineStart = true;
        }

        if (curr < off+len) {
            if (isLineStart) {
                isLineStart = false;
                super.write(prefix, 0, prefix.length());
            }
            super.write(str, curr, off+len-curr);
        }
    }
}
