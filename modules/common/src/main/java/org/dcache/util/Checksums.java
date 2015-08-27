package org.dcache.util;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.Maps.EntryTransformer;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Base64;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Predicates.notNull;
import static com.google.common.base.Strings.nullToEmpty;
import static com.google.common.collect.Collections2.transform;
import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Maps.transformEntries;
import static org.dcache.util.ChecksumType.*;

/**
 * Class containing utility methods for operating on checksum values
 */
public class Checksums
{
    private static final Logger _log = LoggerFactory.getLogger(Checksums.class);

    private static final Splitter.MapSplitter RFC3230_SPLITTER =
            Splitter.on(',').omitEmptyStrings().trimResults().
            withKeyValueSeparator(Splitter.on('=').limit(2));

    private static final EntryTransformer<String,String,Checksum>
            RFC3230_TO_CHECKSUM = new EntryTransformer<String,String,Checksum>(){
            @Override
            public Checksum transformEntry(String type, String value)
            {
                try {
                    /*
                     * These names are defined in RFC-3230 and
                     * http://www.iana.org/assignments/http-dig-alg/http-dig-alg.xml
                     */
                    switch(type.toLowerCase()) {
                    case "adler32":
                        return new Checksum(ChecksumType.ADLER32, value);

                    case "md5":
                        byte[] bytes = Base64.getDecoder().decode(value);
                        return new Checksum(ChecksumType.MD5_TYPE, bytes);

                    default:
                        _log.debug("Unsupported checksum type {}", type);
                        return null;
                    }
                } catch(IllegalArgumentException e) {
                    _log.debug("Value \"{}\" is invalid for type {}", value,
                            type);
                    return null;
                }
            }};
    private static final Ordering<ChecksumType> PREFERRED_CHECKSUM_TYPE_ORDERING =
            Ordering.explicit(MD5_TYPE, ADLER32, MD4_TYPE);
    private static final Ordering<Checksum> PREFERRED_CHECKSUM_ORDERING =
                    PREFERRED_CHECKSUM_TYPE_ORDERING.onResultOf(
                            new Function<Checksum, ChecksumType>()
                            {
                                @Override
                                public ChecksumType apply(Checksum checksum)
                                {
                                    return checksum.getType();
                                }
                            });


    /**
     * This Function maps an instance of Checksum to the corresponding
     * fragment of an RFC 3230 response.
     */
    private static final Function<Checksum,String> TO_RFC3230_FRAGMENT =
            new Function<Checksum,String>() {
        @Override
        public String apply(Checksum f)
        {
            byte[] bytes = f.getBytes();

            switch(f.getType()) {
            case ADLER32:
                return "adler32=" + Checksum.bytesToHexString(bytes);
            case MD4_TYPE:
                return null;
            case MD5_TYPE:
                return "md5=" + Base64.getEncoder().encodeToString(bytes);
            default:
                return null;
            }
        }
    };

    /**
     * This Function maps a collection of Checksum objects to the corresponding
     * RFC 3230 string.  For further details, see:
     *
     *     http://tools.ietf.org/html/rfc3230
     *     http://www.iana.org/assignments/http-dig-alg/http-dig-alg.xml
     */
    public static final Function<Collection<Checksum>,String> TO_RFC3230 =
            new Function<Collection<Checksum>,String>() {
        @Override
        public String apply(Collection<Checksum> checksums)
        {
            Iterable<String> parts = transform(checksums, TO_RFC3230_FRAGMENT);
            return Joiner.on(',').skipNulls().join(parts);
        }
    };

    private Checksums()
    {
        // prevent instantiation
    }

    /**
     * Parse the RFC-3230 Digest response header value.  If there is no
     * understandable checksum or null is supplied then an empty set is
     * returned.
     * @param digest The Digest header value
     * @return the decoded checksum values
     */
    public static Set<Checksum> decodeRfc3230(String digest)
    {
        Map<String,String> parts = RFC3230_SPLITTER.split(nullToEmpty(digest));

        Map<String,Checksum> checksums = transformEntries(parts,
                RFC3230_TO_CHECKSUM);

        return Sets.newHashSet(filter(checksums.values(), notNull()));
    }

    public static Ordering<Checksum> preferrredOrder()
    {
        return PREFERRED_CHECKSUM_ORDERING;
    }
}
