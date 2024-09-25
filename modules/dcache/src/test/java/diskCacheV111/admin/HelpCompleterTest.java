/*
 * dCache - http://www.dcache.org/
 *
 * Copyright (C) 2023 Deutsches Elektronen-Synchrotron
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
package diskCacheV111.admin;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class HelpCompleterTest {

    HelpCompleter helpCompleter = new HelpCompleter("");

    @Test
    public void testScanShouldStripArgs() {
        String line = "log set <appender> [<logger>] OFF|ERROR|WARN|INFO|DEBUG|TRACE|ALL";
        String processed = helpCompleter.scan(line);
        assertEquals("log set", processed);
    }

    @Test
    public void testScanShouldStripWhitespaceSeparatedHyphen() {
        String line = "set max diskspace -";
        String processed = helpCompleter.scan(line);
        assertEquals("set max diskspace", processed);
    }

    @Test
    public void testScanShouldAcceptHyphenContainingCommand() {
        String line = "chimera-maintenance ha show participants";
        String processed = helpCompleter.scan(line);
        assertEquals("chimera-maintenance ha show participants", processed);
    }
}
