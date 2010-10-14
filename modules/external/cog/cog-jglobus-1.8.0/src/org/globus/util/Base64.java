/*
 * Copyright 1999-2006 University of Chicago
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.globus.util;

/**
 * This class provides encode/decode for RFC 2045 Base64 as defined by
 * RFC 2045, N. Freed and N. Borenstein.  <a
 * href="http://www.ietf.org/rfc/rfc2045.txt">RFC 2045</a>:
 * Multipurpose Internet Mail Extensions (MIME) Part One: Format of
 * Internet Message Bodies. Reference 1996
 * <BR>
 * Portion of this file is based on code from Apache Jakarta Project.
 */
public final class  Base64
{
    static private final int  BASELENGTH         = 255;
    static private final byte PAD                = (byte) '=';
    static private byte [] base64Alphabet       = new byte[BASELENGTH];

    static
    {
        for (int i = 0; i < BASELENGTH; i++ )
        {
            base64Alphabet[i] = -1;
        }
        for (int i = 'Z'; i >= 'A'; i--)
        {
            base64Alphabet[i] = (byte) (i - 'A');
        }
        for (int i = 'z'; i>= 'a'; i--)
        {
            base64Alphabet[i] = (byte) (i - 'a' + 26);
        }
        for (int i = '9'; i >= '0'; i--)
        {
            base64Alphabet[i] = (byte) (i - '0' + 52);
        }

        base64Alphabet['+']  = 62;
        base64Alphabet['/']  = 63;
    }

    public static boolean isBase64(String isValidString) {
        return isArrayByteBase64(isValidString.getBytes());
    }

    public static boolean isBase64(byte octect) {
        return (octect == PAD || base64Alphabet[octect] != -1);
    }

    public static boolean isArrayByteBase64(byte[] arrayOctect) {
        int length = arrayOctect.length;
        if (length == 0) {
            // shouldn't a 0 length array be valid base64 data?
            // return false;
            return true;
        }
        for (int i=0; i < length; i++) {
            if ( !Base64.isBase64(arrayOctect[i]) ) {
                return false;
            }
        }
        return true;
    }
    
    /**
     * Encodes hex octects into Base64.
     *
     * @param binaryData Array containing binary data to encode.
     * @return Base64-encoded data.
     */
    public static byte[] encode(byte[] binaryData) {
        return org.bouncycastle.util.encoders.Base64.encode(binaryData);
    }

    /**
     * Decodes Base64 data into octects
     *
     * @param base64Data Byte array containing Base64 data
     * @return Array containing decoded data.
     */
    public static byte[] decode(byte[] base64Data) {
        return org.bouncycastle.util.encoders.Base64.decode(base64Data);
    }

}
