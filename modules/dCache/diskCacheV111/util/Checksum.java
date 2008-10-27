/*
 * @(#)Checksum.java	1.2 03/11/10
 *
 * Copyright 1996-2003 dcache.org All Rights Reserved.
 *
 * This software is the proprietary information of dCache.org
 * Use is subject to license terms.
 */

package diskCacheV111.util;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.prefs.Preferences;

/**
 *
 * @author Patrick Fuhrmann
 * @version 1.2, 03/11/10
 * @see Preferences
 * @since 1.4
 */
public class Checksum {

    private int _type = 0;
    private byte[] _value = null;

    public static final int ADLER32 = 1;
    public static final int MD5 = 2;
    public static final int MD4 = 3;

    private MessageDigest _messageDigest = null;

    /**
     * Create a new checksum instance which simply wraps a MessageDigest
     * instance. ToString() will then deliver a dCache-like checksum with the
     * string format <type>:<hexadecimal digest>. Currently supported
     * algorithms are All
     *
     * @param messageDigest
     *            the digest we are wrapping
     */
    public Checksum(MessageDigest messageDigest)
            throws NoSuchAlgorithmException {
        _messageDigest = messageDigest;
        _type = ChecksumFactory.mapStringTypeToId(messageDigest.getAlgorithm());
    }

    public MessageDigest getMessageDigest() {
        return _messageDigest;
    }

    /**
     * Create a new checksum instance for an already computed digest of a
     * particular type. The length of the digest must match size specified by
     * the algorithm (e.g. 4 byte for Adler32)
     *
     * @param type
     *            the type (or algorithm) of the ckecksum , determines the
     *            digest length
     * @param digest
     *            the binary representation of the digest (checksum)
     */
    public Checksum(int type, byte[] digest) {

        if (digest == null)
            throw new IllegalArgumentException("digest == null");

        _type = type;
        _value = fixPrefix( type, digest.clone() );
    }

    /**
     * Create a new checksum instance for an already computed digest of a
     * particular type. Currently supported are Adler32 (type 1) and MD5 (type
     * 2).
     *
     * @param digestString
     *            the input must have the following format: <type>:<hexadecimal
     *            digest>
     */
    public Checksum(String digestString) {

        if (digestString == null)
            throw new NullPointerException("digestString == null");

        int del = digestString.indexOf(':');
        if (del < 1)
            throw new IllegalArgumentException("Not a dCache checksum (no:)");

        _type = Integer.parseInt(digestString.substring(0, del));

        _value = fixPrefix(_type,  stringToBytes(digestString.substring(del + 1) ) );
    }

    public int getType() {
        return _type;
    }

    @Override
    public boolean equals(Object o) {

        if( o == this ) return true;

        setValue();
        if (!(o instanceof Checksum)) return false;

        Checksum other = (Checksum) o;
        other.setValue();
        if ((_type != other._type) || (_value.length != other._value.length))
            return false;

        for (int i = 0, l = _value.length; i < l; i++)
            if (_value[i] != other._value[i]) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return toString().hashCode();
    }

    public byte[] getDigest() {
        setValue();
        byte[] out = new byte[_value.length];
        System.arraycopy(_value, 0, out, 0, _value.length);
        return out;
    }

    @Override
    public String toString() {
        return "" + _type + ":" + toHexString();
    }

    /**
     * Get the hexadecimal representation of the digest with leading zeros.
     *
     * @return the hex string of the digest
     */
    public String toHexString() {
        setValue();
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < _value.length; i++)
            sb.append(byteToHexString(_value[i]));

        return sb.toString();
    }

    static public String toHexString(byte[] value) {
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < value.length; i++)
            sb.append(byteToHexString(value[i]));

        return sb.toString();
    }

    private void setValue() { // ensure that _value is set only once as
                                // MessageDigest object resets after "digest"
                                // method call
        if (_value == null && _messageDigest != null)
            _value = _messageDigest.digest();
    }

    private static final String[] __map = { "0", "1", "2", "3", "4", "5", "6",
            "7", "8", "9", "a", "b", "c", "d", "e", "f" };

    static public String byteToHexString(byte b) {

        // int x = ( b < 0 ) ? ( 256 + (int)b ) : (int)b ;

        return __map[(b >> 4) & 0xf] + __map[(b) & 0xf];
    }

    static public byte[] stringToBytes(String str) {
        if (str == null) throw new NullPointerException("String == null");

        if ((str.length() % 2) != 0) str = "0" + str;

        byte[] r = new byte[str.length() / 2];

        for (int i = 0, l = str.length(); i < l; i += 2) {
            r[i / 2] = (byte) Integer.parseInt(str.substring(i, i + 2), 16);
        }
        return r;

    }

    /**
     * due to bug in checksum calculation module, some ADLER32 sums stored without
     * leading zeros.
     *
     * @param type
     * @param b
     * @return
     */
    private static byte[] fixPrefix(int type, byte[] b) {


        if( type == ADLER32 && b.length < 4 ) {
            byte[] n = new byte[] { 0, 0, 0, 0};
            System.arraycopy(b, 0, n, n.length - b.length , b.length);
            return n;
        }
        return b;

    }

    public static void main(String[] args) throws Exception {

        byte[] input = new byte[] { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };

        Adler32 adler = new Adler32();
        adler.update(input);
        Checksum c1 = new Checksum(1, adler.digest());
        System.out.println("c1 : computed by Adler32-class");
        System.out.println(c1);

        System.out.println("\nc2 : computed by Adler32-class");
        Checksum c2 = new Checksum(new Adler32());
        c2.getMessageDigest().update(input);
        System.out.println(c2);
        System.out.println("c1 = c2 ? " + (c1.equals(c2) && c2.equals(c1)));

        System.out.println("\nc3 : '1:a0001' adler32");
        Checksum c3 = new Checksum("1:a0001");
        System.out.println(c3);
        System.out.println("c1 = c3 ? " + (c3.equals(c1) && c1.equals(c3)));

        System.out.println("\nc4 : '1:0a0001' adler32");
        Checksum c4 = new Checksum("1:0a0001");
        System.out.println(c3);
        System.out.println("c1 = c4 ? " + (c4.equals(c1) && c1.equals(c4)));

        System.out.println("\nc5 : '1:000a0001' adler32");
        Checksum c5 = new Checksum("1:000a0001");
        System.out.println(c5);
        System.out.println("c1 = c5 ? " + (c5.equals(c1) && c1.equals(c5)));

        System.out.println("\nc6 : '1:0000a0001' adler32");
        Checksum c6 = new Checksum("1:0000a0001");
        System.out.println(c6);
        System.out.println("c1 = c6 ? " + (c6.equals(c1) && c1.equals(c6)));

        System.out.println("\nc7 : '2:49f83ad0f6526ae19e351853ec3bf' md5");
        System.out.println(new Checksum("2:49f83ad0f6526ae19e351853ec3bf"));

        System.out.println("\nc8 : '1:bc389613' adler32");
        System.out.println(new Checksum("1:bc389613"));

        System.out.println("\nc9 : '1:fFfFFFff' adler32");
        System.out.println(new Checksum("1:fFfFFFff"));

        System.out.println("\nc10 : '1:00000001' adler32");
        System.out.println(new Checksum("1:00000001"));

        System.out.println("\nc11 : '1:0000000000' adler32");
        try {
            new Checksum("1:0000000000");
        } catch (IllegalArgumentException e) {
            System.out.println(e.getMessage());
        }

        System.out.println("\nc12 : '1:00F000000000' adler32");
        try {
            new Checksum("1:00F000000000");
        } catch (IllegalArgumentException e) {
            System.out.println(e.getMessage());
        }
    }
}
