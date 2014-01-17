/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2013 Deutsches Elektronen-Synchrotron
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

import jline.ANSIBuffer;

import org.dcache.commons.util.Strings;

/**
 * Utility class to produce help texts suitable for an ANSI terminal.
 */
public class AnsiHelpPrinter extends TextHelpPrinter
{
    @Override
    protected int plainLength(String s)
    {
        return Strings.plainLength(s);
    }

    @Override
    protected String value(String value)
    {
        return new ANSIBuffer().underscore(value.toLowerCase()).toString();
    }

    @Override
    protected String literal(String option)
    {
        return new ANSIBuffer().bold(option).toString();
    }

    @Override
    protected String heading(String heading)
    {
        return new ANSIBuffer().bold(heading).toString();
    }
}
