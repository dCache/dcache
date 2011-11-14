package org.dcache.util;

import java.io.Serializable;

import static org.dcache.util.ChecksumType.*;

public class Checksum  implements Serializable
{
    static final long serialVersionUID = 7338775749513974986L;

    private final static char DELIMITER = ':';
    private final static String[] HEX = {
        "0", "1", "2", "3", "4", "5", "6", "7", "8", "9",
        "a", "b", "c", "d", "e", "f"
    };

    private final ChecksumType type;
    private final String value;

    /** Creates a new instance of Checksum */
    public Checksum(ChecksumType type, byte[] value)
    {
        this(type, bytesToHexString(value));
    }

    /** Creates a new instance of Checksum */
    public Checksum(ChecksumType type, String value)
    {
        if (value == null) {
            throw new IllegalArgumentException("Null value is not allowed");
        }

        /**
         * Due to bug in checksum calculation module, some ADLER32
         * sums are stored without leading zeros.
         */
        if (type == ADLER32 && value.length() < 8) {
            char[] newValue =
                new char[] { '0', '0', '0', '0', '0', '0', '0', '0' };
            value.getChars(0, value.length(),
                           newValue, newValue.length - value.length());
            value = String.valueOf(newValue);
        }
        this.type = type;
        this.value = value;
    }

    public ChecksumType getType()
    {
        return type;
    }

    public String getValue()
    {
        return value;
    }

    public byte[] getBytes()
    {
        return stringToBytes(value);
    }

    @Override
    public boolean equals(Object other)
    {
        if(other == null){
            return false;
        }

        if (other == this) {
            return true;
        }

        if (!(other.getClass().equals(Checksum.class))) {
            return false;
        }

        Checksum that = (Checksum) other;
        return ((this.type == that.type) && this.value.equals(that.value));
    }

    @Override
    public int hashCode()
    {
        return value.hashCode() ^ type.hashCode();
    }

    @Override
    public String toString()
    {
        return toString(false);
    }

    public String toString(boolean useStringKey)
    {
        return (useStringKey ? type.getName() : String.valueOf(type.getType())) + ":" + value;
    }

    private static String byteToHexString(byte b)
    {
        return HEX[(b >> 4) & 0xf] + HEX[(b) & 0xf];
    }

    public static String bytesToHexString(byte[] value)
    {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < value.length; i++) {
            sb.append(byteToHexString(value[i]));
        }
        return sb.toString();
    }

    private static byte[] stringToBytes(String str)
    {
        if ((str.length() % 2) != 0) {
            str = "0" + str;
        }

        byte[] r = new byte[str.length() / 2];

        for (int i = 0, l = str.length(); i < l; i += 2) {
            r[i / 2] = (byte) Integer.parseInt(str.substring(i, i + 2), 16);
        }
        return r;
    }

    /**
     * Create a new checksum instance for an already computed digest
     * of a particular type.
     *
     * @param digest the input must have the following format:
     *            <type>:<hexadecimal digest>
     */
    public static Checksum parseChecksum(String digest)
    {
        int del = digest.indexOf(DELIMITER);
        if (del < 1) {
            throw new IllegalArgumentException("Not a dCache checksum");
        }

        String type = digest.substring(0, del);
        String checksum = digest.substring(del + 1);

        return new Checksum(ChecksumType.getChecksumType(type), checksum);
    }
}
