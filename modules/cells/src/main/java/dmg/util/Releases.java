/*
 * dCache - http://www.dcache.org/
 *
 * Copyright (C) 2016 Deutsches Elektronen-Synchrotron
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

package dmg.util;

public abstract class Releases
{
    public static final short RELEASE_2_10 = 0x020A;
    public static final short RELEASE_2_11 = 0x020B;
    public static final short RELEASE_2_12 = 0x020C;
    public static final short RELEASE_2_13 = 0x020D;
    public static final short RELEASE_2_14 = 0x020E;
    public static final short RELEASE_2_15 = 0x020F;
    public static final short RELEASE_2_16 = 0x0210;

    public static short getRelease(String version)
    {
        int i = version.indexOf(".");
        if (i < 0) {
            throw new NumberFormatException("Invalid dCache version: " + version);
        }
        int j = version.indexOf(".", i + 1);
        return j < 0
               ? (short) (Short.parseShort(version.substring(0, i)) << 8)
               : (short) ((Short.parseShort( version.substring(0, i)) << 8) | Short.parseShort(version.substring(i + 1, j)));
    }
}