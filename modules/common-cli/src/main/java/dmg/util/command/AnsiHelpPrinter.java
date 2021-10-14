/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2013 -2016 Deutsches Elektronen-Synchrotron
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
package dmg.util.command;

import static org.fusesource.jansi.Ansi.Attribute.UNDERLINE;
import static org.fusesource.jansi.Ansi.Attribute.UNDERLINE_OFF;
import static org.fusesource.jansi.Ansi.ansi;

import org.dcache.util.Strings;

/**
 * Utility class to produce help texts suitable for an ANSI terminal.
 */
public class AnsiHelpPrinter extends TextHelpPrinter {

    @Override
    protected int plainLength(String s) {
        return Strings.plainLength(s);
    }

    @Override
    protected String value(String value) {
        return ansi().a(UNDERLINE).a(value.toLowerCase()).a(UNDERLINE_OFF).toString();
    }

    @Override
    protected String literal(String option) {
        return ansi().bold().a(option).boldOff().toString();
    }

    @Override
    protected String heading(String heading) {
        return ansi().bold().a(heading).boldOff().toString();
    }
}
