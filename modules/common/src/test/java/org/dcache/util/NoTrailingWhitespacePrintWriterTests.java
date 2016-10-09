/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2001 - 2016 Deutsches Elektronen-Synchrotron
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

import java.io.PrintWriter;
import java.io.StringWriter;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class NoTrailingWhitespacePrintWriterTests
{
    PrintWriter writer;
    StringWriter result;

    @Before
    public void setup()
    {
        result = new StringWriter();
        writer = new NoTrailingWhitespacePrintWriter(result);
    }

    @Test
    public void shouldAppendNoWS()
    {
        writer.append("HELLO");

        assertThat(result.toString(), is(equalTo("HELLO")));
    }

    @Test
    public void shouldAppendTrailingWS()
    {
        writer.append("HELLO ");

        assertThat(result.toString(), is(equalTo("HELLO")));
    }

    @Test
    public void shouldNotAppendOnlyWS()
    {
        writer.append(" ");

        assertThat(result.toString(), is(equalTo("")));
    }

    @Test
    public void shouldAppendTrailingWSAfterAppend()
    {
        writer.append("HELLO ").append("WORLD");

        assertThat(result.toString(), is(equalTo("HELLO WORLD")));
    }

    @Test
    public void shouldAppendOnlyWSAfterAppend()
    {
        writer.append(" ").append("HELLO");

        assertThat(result.toString(), is(equalTo(" HELLO")));
    }

    @Test
    public void shouldPrintNoWS()
    {
        writer.print("HELLO");

        assertThat(result.toString(), is(equalTo("HELLO")));
    }

    @Test
    public void shouldPrintTrailingWS()
    {
        writer.print("HELLO ");

        assertThat(result.toString(), is(equalTo("HELLO")));
    }

    @Test
    public void shouldNotPrintOnlyWS()
    {
        writer.print(" ");

        assertThat(result.toString(), is(equalTo("")));
    }

    @Test
    public void shouldPrintTrailingWSAfterPrint()
    {
        writer.print("HELLO ");
        writer.print("WORLD");

        assertThat(result.toString(), is(equalTo("HELLO WORLD")));
    }

    @Test
    public void shouldPrintOnlyWSAfterPrint()
    {
        writer.print(" ");
        writer.append("HELLO");

        assertThat(result.toString(), is(equalTo(" HELLO")));
    }

    @Test
    public void shouldSkipWSBeforePrintln()
    {
        writer.append("HELLO ");
        writer.println();
        writer.print("WORLD");

        assertThat(result.toString(), is(equalTo("HELLO\nWORLD")));
    }

    @Test
    public void shouldPrintCharSkipTrailingWS()
    {
        writer.print('H');
        writer.print(' ');

        assertThat(result.toString(), is(equalTo("H")));
    }

    @Test
    public void shouldPrintCharIncludeNonTrailingWS()
    {
        writer.print('H');
        writer.print(' ');
        writer.print('W');

        assertThat(result.toString(), is(equalTo("H W")));
    }

    @Test
    public void shouldPrintCharArraySkipTrailingWS()
    {
        writer.print("H ".toCharArray());

        assertThat(result.toString(), is(equalTo("H")));
    }

    @Test
    public void shouldPrintCharArrayIncludeTrailingWSWithFollowingText()
    {
        writer.print("H ".toCharArray());
        writer.print("W");

        assertThat(result.toString(), is(equalTo("H W")));
    }

    @Test
    public void shouldOmitOnlyWSCharArray()
    {
        writer.print(" ".toCharArray());

        assertThat(result.toString(), is(equalTo("")));
    }

    @Test
    public void shouldPrintOnlyWSCharArrayWithFollowingText()
    {
        writer.print(" ".toCharArray());
        writer.print("W");

        assertThat(result.toString(), is(equalTo(" W")));
    }

    @Test
    public void shouldPrintCharArrayIncludeNonTrailingWS()
    {
        writer.print("H W".toCharArray());

        assertThat(result.toString(), is(equalTo("H W")));
    }

    @Test
    public void shouldAppendPartialStringWithZeroOffset()
    {
        writer.write("HELLO", 0, 4);

        assertThat(result.toString(), is(equalTo("HELL")));
    }

    @Test
    public void shouldAppendPartialStringWithNonZeroOffset()
    {
        writer.write("HELLO", 1, 3);

        assertThat(result.toString(), is(equalTo("ELL")));
    }

    @Test
    public void shouldAppendPartialStringWithNonZeroOffsetRemainingString()
    {
        writer.write("HELLO", 1, 4);

        assertThat(result.toString(), is(equalTo("ELLO")));
    }
}
