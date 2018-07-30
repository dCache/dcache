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

import java.util.Optional;

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
     * Any class that describes a textual representation of ByteUnit.  This
     * allows conversion between the ByteUnit and that String representation.
     * <p>
     * It is required that {@literal r.parse(r.of(unit)) == unit} for all
     * {@literal ByteUnit unit} and {@literal Representation r} where
     * {@literal r.of(unit)} returns non-exceptionally.
     * <p>
     * It is allowed that the parse method returns the same ByteUnit value for
     * different String values.  This allows for common aliases; for example,
     * parsing {@literal "kB"} (under JEDEC) as equivalent to the correct
     * representation: {@literal "KB"}.
     */
    public interface Representation
    {
        /**
         * Provide the String representation of a particular ByteUnit.
         * @param unit the ByteUnit to represent
         * @return the corresponding String representation.
         * @throws UnsupportedOperationException if there is no representation for this ByteUnit.
         */
        String of(ByteUnit unit);

        /**
         * Parse a representation of a ByteUnit.  The value must match exactly.
         * @param value The String representation
         * @return The corresponding ByteUnit, if one matches.
         * @throws NullPointerException if the value is null.
         */
        Optional<ByteUnit> parse(String value);
    }

    /**
     * Just the ISO prefix of a ByteUnit ("k", "Ki", "M", "Mi", ...).  The
     * ByteUnit.BYTES unit is represented by the empty String.
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

            case EiB:
                return "Ei";

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

            case EB:
                return "E";

            default:
                throw new UnsupportedOperationException("no ISO prefix for " + unit.name());
            }
        }

        @Override
        public Optional<ByteUnit> parse(String value)
        {
            switch (value) {
            case "":
                return Optional.of(ByteUnit.BYTES);
            // Note that the IEC symbol for kibi is defined as "Ki" and not "ki"!
            case "Ki":
                return Optional.of(ByteUnit.KiB);
            case "Mi":
                return Optional.of(ByteUnit.MiB);
            case "Gi":
                return Optional.of(ByteUnit.GiB);
            case "Ti":
                return Optional.of(ByteUnit.TiB);
            case "Pi":
                return Optional.of(ByteUnit.PiB);
            case "Ei":
                return Optional.of(ByteUnit.EiB);
            case "k":
                return Optional.of(ByteUnit.KB);
            case "M":
                return Optional.of(ByteUnit.MB);
            case "G":
                return Optional.of(ByteUnit.GB);
            case "T":
                return Optional.of(ByteUnit.TB);
            case "P":
                return Optional.of(ByteUnit.PB);
            case "E":
                return Optional.of(ByteUnit.EB);
            }
            return Optional.empty();
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

            case EiB:
                return "EiB";

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

            case EB:
                return "EB";

            default:
                throw new UnsupportedOperationException("no ISO unit for " + unit.name());
            }
        }

        @Override
        public Optional<ByteUnit> parse(String value)
        {
            switch (value) {
            case "B":
                return Optional.of(ByteUnit.BYTES);
            // Note that the IEC symbol for kibi is defined as "Ki" and not "ki"!
            case "KiB":
                return Optional.of(ByteUnit.KiB);
            case "MiB":
                return Optional.of(ByteUnit.MiB);
            case "GiB":
                return Optional.of(ByteUnit.GiB);
            case "TiB":
                return Optional.of(ByteUnit.TiB);
            case "PiB":
                return Optional.of(ByteUnit.PiB);
            case "EiB":
                return Optional.of(ByteUnit.EiB);
            case "kB":
                return Optional.of(ByteUnit.KB);
            case "MB":
                return Optional.of(ByteUnit.MB);
            case "GB":
                return Optional.of(ByteUnit.GB);
            case "TB":
                return Optional.of(ByteUnit.TB);
            case "PB":
                return Optional.of(ByteUnit.PB);
            case "EB":
                return Optional.of(ByteUnit.EB);
            }
            return Optional.empty();
        }
    }

    /**
     * Provides the JEDEC prefix of a ByteUnit ("K", "M", "G", ...). The
     * ByteUnit.BYTES unit is represented by the empty String.
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

            case EiB:
                return "E";

            default:
                throw new UnsupportedOperationException("no JEDEC prefix for " + unit.name());
            }
        }

        @Override
        public Optional<ByteUnit> parse(String value)
        {
            switch (value) {
            case "":
                return Optional.of(ByteUnit.BYTES);

            // NB. JEDEC label is upper-case K, but as this is often confused
            // we also accept lower-case k.
            case "k":
            case "K":
                return Optional.of(ByteUnit.KiB);
            case "M":
                return Optional.of(ByteUnit.MiB);
            case "G":
                return Optional.of(ByteUnit.GiB);
            case "T":
                return Optional.of(ByteUnit.TiB);
            case "P":
                return Optional.of(ByteUnit.PiB);
            case "E":
                return Optional.of(ByteUnit.EiB);
            }
            return Optional.empty();
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

            case EiB:
                return "EB";

            default:
                throw new UnsupportedOperationException("no JEDEC unit for " + unit.name());
            }
        }

        @Override
        public Optional<ByteUnit> parse(String value)
        {
            switch (value) {
            case "B":
                return Optional.of(ByteUnit.BYTES);

            // NB. JEDEC label is upper-case K, but as this is often confused
            // we also accept lower-case k.
            case "kB":
            case "KB":
                return Optional.of(ByteUnit.KiB);

            case "MB":
                return Optional.of(ByteUnit.MiB);

            case "GB":
                return Optional.of(ByteUnit.GiB);

            case "TB":
                return Optional.of(ByteUnit.TiB);

            case "PB":
                return Optional.of(ByteUnit.PiB);

            case "EB":
                return Optional.of(ByteUnit.EiB);
            }
            return Optional.empty();
        }
    }

    /**
     * Provide the ISO prefix of a ByteUnit; i.e., without the final
     * units ('B').  Returns SI symbols (e.g., "k" for KILOBYTES, "M" for
     * MEGABYTES) for Type.DECIMAL, and IEC symbols (e.g., "Ki" KIBIBYTES,
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
