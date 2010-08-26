package diskCacheV111.util;

/**
 * $Id$
 */
public class PnfsIdUtil {

    /**
     * Translation table used by bytesToHexString.
     */
    private static final char valueToHex[] =
            {'0', '1', '2', '3', '4', '5','6', '7', '8', '9', 'A', 'B','C', 'D', 'E', 'F' };

    private static int hexToValue(byte val) {
	if (val >= '0' && val <= '9') {
	    val -= '0';
  	    return val;
	} else if (val >= 'a' && val <= 'f') {
	    val -= 'a' - 10;
  	    return val;
	} else if (val >= 'A' && val <= 'F') {
	    val -= 'A' - 10;
	    return val;
	}
	return -1;
    }


    /**
     * Converts string representation of pnfsid into its internal binary form used by PNFS server
     *
     * @param pnfsid as String
     * @return pnfsid as byte array
     */
    public static byte[] toBinPnfsId(String pnfsid) {
	byte[] bb = pnfsid.getBytes();
	byte[] ba = new byte[bb.length/2];
	//
	ba[ 0] = (byte)(hexToValue(bb[ 2])<<4 | hexToValue(bb[ 3]));
	ba[ 1] = (byte)(hexToValue(bb[ 0])<<4 | hexToValue(bb[ 1]));
	//
	ba[ 2] = (byte)(hexToValue(bb[ 6])<<4 | hexToValue(bb[ 7]));
	ba[ 3] = (byte)(hexToValue(bb[ 4])<<4 | hexToValue(bb[ 5]));
	//
	ba[ 4] = (byte)(hexToValue(bb[14])<<4 | hexToValue(bb[15]));
	ba[ 5] = (byte)(hexToValue(bb[12])<<4 | hexToValue(bb[13]));
	ba[ 6] = (byte)(hexToValue(bb[10])<<4 | hexToValue(bb[11]));
	ba[ 7] = (byte)(hexToValue(bb[ 8])<<4 | hexToValue(bb[ 9]));
	//
	ba[ 8] = (byte)(hexToValue(bb[22])<<4 | hexToValue(bb[23]));
	ba[ 9] = (byte)(hexToValue(bb[20])<<4 | hexToValue(bb[21]));
	ba[10] = (byte)(hexToValue(bb[18])<<4 | hexToValue(bb[19]));
	ba[11] = (byte)(hexToValue(bb[16])<<4 | hexToValue(bb[17]));
	return ba;
    }


    /**
     * Converts binary representation of pnfsid used by PNFS server into string used by dCache code
     *
     * @param pnfsid as byte array
     * @return pnfsid as String
     */
    public static String toStringPnfsId(byte[] pnfsid) {
        char result[] = new char[2 * pnfsid.length];
	int i;
        for (i = 0; i < 2; i++) {
            int value = (pnfsid[1-i] + 0x100) & 0xFF;
            result[2 * i    ] = valueToHex[value >> 4];
            result[2 * i + 1] = valueToHex[value & 0x0F];
        }
        for (     ; i < 4; i++) {
            int value = (pnfsid[5-i] + 0x100) & 0xFF;
            result[2 * i    ] = valueToHex[value >> 4];
            result[2 * i + 1] = valueToHex[value & 0x0F];
        }
        for (     ; i < 8; i++) {
            int value = (pnfsid[11-i] + 0x100) & 0xFF;
            result[2 * i    ] = valueToHex[value >> 4];
            result[2 * i + 1] = valueToHex[value & 0x0F];
        }
        for (    ; i < 12; i++) {
            int value = (pnfsid[19-i] + 0x100) & 0xFF;
            result[2 * i    ] = valueToHex[value >> 4];
            result[2 * i + 1] = valueToHex[value & 0x0F];
        }
        return new String(result);
    }
}
