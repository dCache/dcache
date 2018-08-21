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

import org.junit.Before;
import org.junit.Test;

import java.io.StringWriter;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.*;

public class LineIndentingPrintWriterTest
{
    LineIndentingPrintWriter pw;
    StringWriter sw;

    @Before
    public void setup()
    {
        sw = new StringWriter();
        pw = new LineIndentingPrintWriter(sw, "    ");
    }

    @Test
    public void shouldIndentWriteString()
    {
        pw.write("hello");

        assertThat(sw.toString(), is(equalTo("    hello")));
    }

    @Test
    public void shouldIndentWriteWriteString()
    {
        pw.write("hello");
        pw.write(", world");

        assertThat(sw.toString(), is(equalTo("    hello, world")));
    }

    @Test
    public void shouldIndentEmbeddedLine()
    {
        pw.write("hello\nworld");

        assertThat(sw.toString(), is(equalTo("    hello\n    world")));
    }

    @Test
    public void shouldNotIndentEmptyLine()
    {
        pw.write("hello\nworld\n");

        assertThat(sw.toString(), is(equalTo("    hello\n    world\n")));
    }

    @Test
    public void shouldIndentSubsequentLine()
    {
        pw.write("hello\nworld\n");
        pw.write("There");

        assertThat(sw.toString(), is(equalTo("    hello\n    world\n    There")));
    }

    @Test
    public void shouldIndentCharacter()
    {
        pw.write('a');

        assertThat(sw.toString(), is(equalTo("    a")));
    }

    @Test
    public void shouldAcceptNewlineCharacter()
    {
        pw.write('a');
        pw.write('\n');

        assertThat(sw.toString(), is(equalTo("    a\n")));
    }

    @Test
    public void shouldIndentLineAfterNewlineCharacter()
    {
        pw.write('a');
        pw.write('\n');
        pw.write('b');

        assertThat(sw.toString(), is(equalTo("    a\n    b")));
    }

    @Test
    public void shouldSuppressEmptyLineIndent()
    {
        pw.write('\n');
        pw.write('b');

        assertThat(sw.toString(), is(equalTo("\n    b")));
    }

    @Test
    public void shouldIndentPartialCharArray()
    {
        pw.write("abcd".toCharArray(), 1, 2);

        assertThat(sw.toString(), is(equalTo("    bc")));
    }

    @Test
    public void shouldNotIndentPartialCharArrayWithFinalNewline()
    {
        pw.write("abcd\nef".toCharArray(), 1, 4);

        assertThat(sw.toString(), is(equalTo("    bcd\n")));
    }

    @Test
    public void shouldRememberEmbeddedNewlineWithinPartialCharArray()
    {
        pw.write("abcd\nef".toCharArray(), 1, 4);
        pw.write("test");

        assertThat(sw.toString(), is(equalTo("    bcd\n    test")));
    }

    @Test
    public void shouldIndentEmbeddedNewlineWithinPartialCharArray()
    {
        pw.write("abcd\nef".toCharArray(), 1, 5);

        assertThat(sw.toString(), is(equalTo("    bcd\n    e")));
    }

    @Test
    public void shouldIndentPartialString()
    {
        pw.write("world", 1, 2);

        assertThat(sw.toString(), is(equalTo("    or")));
    }

    @Test
    public void shouldRememberEmbeddedNewlineWithinPartialString()
    {
        pw.write("abcd\nef", 1, 4);
        pw.write("test");

        assertThat(sw.toString(), is(equalTo("    bcd\n    test")));
    }

    @Test
    public void shouldIndentEmbeddedNewlineWithinPartialString()
    {
        pw.write("abcd\nef", 1, 5);

        assertThat(sw.toString(), is(equalTo("    bcd\n    e")));
    }
}
