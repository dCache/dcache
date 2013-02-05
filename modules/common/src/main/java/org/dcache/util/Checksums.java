package org.dcache.util;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import static com.google.common.collect.Collections2.transform;
import static com.google.common.base.Strings.nullToEmpty;
import diskCacheV111.util.Base64;
import java.util.Collection;


/**
 * Class containing utility methods for operating on checksum values
 */
public class Checksums
{
    private Checksums()
    {
        // prevent instantiation
    }

    /**
     * This Function maps an instance of Checksum to the corresponding
     * fragment of an RFC 3230 response.  For further details, see:
     *
     *     http://tools.ietf.org/html/rfc3230
     *     http://www.iana.org/assignments/http-dig-alg/http-dig-alg.xml
     */
    private static final Function<Checksum,String> FOR_RFC3230 =
            new Function<Checksum,String>() {
        @Override
        public String apply(Checksum f)
        {
            byte[] bytes = f.getBytes();

            switch(f.getType()) {
                case ADLER32:
                    return "adler32:" + Checksum.bytesToHexString(bytes);
                case MD4_TYPE:
                    return null;
                case MD5_TYPE:
                    return "md5:" + Base64.byteArrayToBase64(bytes);
                default:
                    return null;
            }
        }
    };


    public static String rfc3230Encoded(Checksum checksum)
    {
        return nullToEmpty(FOR_RFC3230.apply(checksum));
    }

    public static String rfc3230Encoded(Collection<Checksum> checksums)
    {
        Iterable<String> rfc3230Parts = transform(checksums, FOR_RFC3230);
        return Joiner.on(',').skipNulls().join(rfc3230Parts);
    }
}
