/* dCache - http://www.dcache.org/
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
package org.dcache.util;

/**
 * Utility class to work with a ByteUnit
 */
public class ByteUnits
{
    private static final Representation ISO_PREFIX = new IsoPrefix();
    private static final Representation JEDEC_PREFIX = new JedecPrefix();
    private static final Representation ISO_SYMBOL = new IsoUnit();
    private static final Representation JEDEC_SYMBOL = new JedecUnit();

    private ByteUnits()
    {
        /* Prevent instantiation */
    }

    /**
     * Any class that converts a ByteUnit into some String representation.
     */
    public interface Representation
    {
        String of(ByteUnit unit);
    }

    /**
     * Provides just the ISO prefix of a ByteUnit ("k", "Ki", "M",
     * "Mi", ...).
     */
    public static class IsoPrefix implements Representation
    {
        @Override
        public String of(ByteUnit unit)
        {
            switch (unit) {
            case BYTES:
                return "";

            case KiB:
                return "Ki";

            case MiB:
                return "Mi";

            case GiB:
                return "Gi";

            case TiB:
                return "Ti";

            case PiB:
                return "Pi";

            case KB:
                return "k";

            case MB:
                return "M";

            case GB:
                return "G";

            case TB:
                return "T";

            case PB:
                return "P";

            default:
                throw new UnsupportedOperationException("no ISO prefix for " + unit.name());
            }
        }
    }

    /**
     * Provides the ISO representation of a ByteUnit ("kB", "KiB",
     * "MB", "MiB", ...).
     */
    public static class IsoUnit implements Representation
    {
        @Override
        public String of(ByteUnit unit)
        {
            switch (unit) {
            case BYTES:
                return "B";

            case KiB:
                return "KiB";

            case MiB:
                return "MiB";

            case GiB:
                return "GiB";

            case TiB:
                return "TiB";

            case PiB:
                return "PiB";

            case KB:
                return "kB";

            case MB:
                return "MB";

            case GB:
                return "GB";

            case TB:
                return "TB";

            case PB:
                return "PB";

            default:
                throw new UnsupportedOperationException("no ISO unit for " + unit.name());
            }
        }
    }

    /**
     * Provides the JEDEC prefix of a ByteUnit ("K", "M", "G", ...).
     */
    public static class JedecPrefix implements Representation
    {
        @Override
        public String of(ByteUnit unit)
        {
            switch (unit) {
            case BYTES:
                return "";

            // NB. JEDEC label is upper-case K, but this is often confused.
            case KiB:
                return "K";

            case MiB:
                return "M";

            case GiB:
                return "G";

            case TiB:
                return "T";

            case PiB:
                return "P";

            default:
                throw new UnsupportedOperationException("no JEDEC prefix for " + unit.name());
            }
        }
    }

    /**
     * Provides the JEDEC representation of a ByteUnit ("KB", "MB", "GB",
     * ...).
     */
    public static class JedecUnit implements Representation
    {
        @Override
        public String of(ByteUnit unit)
        {
            switch (unit) {
            case BYTES:
                return "B";

            case KiB:
                return "KB";

            case MiB:
                return "MB";

            case GiB:
                return "GB";

            case TiB:
                return "TB";

            case PiB:
                return "PB";

            default:
                throw new UnsupportedOperationException("no JEDEC unit for " + unit.name());
            }
        }
    }

    /**
     * Provide the ISO prefix of a ByteUnit; i.e., without the final
     * units ('B').  Returns SI symbols (e.g., "k" for KILOBYTES, "M" for
     * MEGABYTES) for Type.DECIMAL, and IEC symbols (e.g., "ki" KIBIBYTES,
     * "Mi" for MEBIBYTES) for Type.BINARY.  BYTES returns an empty string.
     */
    public static Representation isoPrefix()
    {
        return ISO_PREFIX;
    }

    /**
     * Provides the JEDEC prefix of a ByteUnit; i.e., without the final
     * units ('B').  Returns JEDEC symbols (e.g., "K" for KIBIBYTES, "M" for
     * MEBIBYTES) for Type.BINARY and throws an except for Type.DECIMAL.
     * BYTES returns an empty string.
     */
    public static Representation jedecPrefix()
    {
        return JEDEC_PREFIX;
    }

    /**
     * The complete ISO representation, including units ('B').  Returns SI
     * symbols (e.g., "kB" for KILOBYTES, "MB" for MEGABYTES) for Type.DECIMAL,
     * and IEC symbols (e.g., "KiB" KIBIBYTES, "MiB" for MEBIBYTES) for
     * Type.BINARY.  BYTES returns an empty string.
     */
    public static Representation isoSymbol()
    {
        return ISO_SYMBOL;
    }

    /**
     * The complete JEDEC representation, including units ('B').  Returns
     * JEDEC symbols (e.g., "KB" for KIBIBYTES, "MB" for MEBIBYTES) for
     * Type.BINARY and throws an except for Type.DECIMAL.  BYTES returns an
     * empty string.
     */
    public static Representation jedecSymbol()
    {
        return JEDEC_SYMBOL;
    }
}
