package org.dcache.util;

/**
 * This enum contains information about Checksum types that dCache understands.
 * It also contains some static information about the different checksum types
 */
public enum ChecksumType
{
    ADLER32(1,"ADLER32", 32),
    MD5_TYPE(2,"MD5", 128),
    MD4_TYPE(3,"MD4", 128);

    private final int type;
    private final String name;
    private final int bits;


    /** Creates a new instance of Checksum */
    ChecksumType(int type, String name, int bits)
    {
        this.type = type;
        this.name = name;
        this.bits = bits;
    }

    /**
     * Convert the internal dCache code for a checksum type to the
     * corresponding ChecksumType object.
     * @throws IllegalArgumentException if the code does not correspond to a
     * checksum type.
     */
    public static final ChecksumType getChecksumType(int i)
    {
        for (ChecksumType type: ChecksumType.values()) {
            if (type.type == i) {
                return type;
            }
        }
        throw new IllegalArgumentException("Unknown checksum type: " + i);
    }


    /**
     * Convert a String containing the dCache internal canonical name for
     * this checksum type to the corresponding value.  The case of the name
     * is ignored.
     * @throws IllegalArgumentException if the name is unknown
     */
    public static final ChecksumType getChecksumType(String s)
    {
        for (ChecksumType type: ChecksumType.values()) {
            if (type.name.equalsIgnoreCase(s) ||
                String.valueOf(type.type).equals(s)) {
                return type;
            }
        }
        throw new IllegalArgumentException("Unknown checksum type: " + s);
    }

    /**
     * Return an dCache internal code for this checksum type.
     */
    public int getType()
    {
        return type;
    }

    /**
     * Return a dCache internal canonical name for this checksum type.
     */
    public String getName()
    {
        return name;
    }

    /**
     * Return the number of binary digits needed to represent values of this
     * checksum type.
     */
    public int getBits()
    {
        return bits;
    }

    /**
     * Return the number of hexadecimal digits needed to represent values
     * of this checksum type.
     */
    public int getNibbles()
    {
        return (bits + 3) / 4;
    }

    @Override
    public String toString()
    {
        return getName();
    }
}
