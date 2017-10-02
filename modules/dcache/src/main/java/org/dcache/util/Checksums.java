package org.dcache.util;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.Maps.EntryTransformer;
import com.google.common.collect.Ordering;
import com.google.common.io.BaseEncoding;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.Base64;
import java.util.Collection;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.dcache.vehicles.FileAttributes;

import static com.google.common.base.Strings.nullToEmpty;
import static com.google.common.collect.Collections2.transform;
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
            RFC3230_TO_CHECKSUM = (type, value) -> {
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
            };
    private static final Ordering<ChecksumType> PREFERRED_CHECKSUM_TYPE_ORDERING =
            Ordering.explicit(MD5_TYPE, ADLER32, MD4_TYPE);
    private static final Ordering<Checksum> PREFERRED_CHECKSUM_ORDERING =
            PREFERRED_CHECKSUM_TYPE_ORDERING.onResultOf(Checksum::getType);


    /**
     * This Function maps an instance of Checksum to the corresponding
     * fragment of an RFC 3230 response.
     */
    private static final Function<Checksum,String> TO_RFC3230_FRAGMENT =
            f -> {
                String value = f.getValue();

                switch(f.getType()) {
                case ADLER32:
                    return "adler32=" + value;
                case MD4_TYPE:
                    return null;
                case MD5_TYPE:
                    byte[] bytes = BaseEncoding.base16().lowerCase().decode(value);
                    return "md5=" + Base64.getEncoder().encodeToString(bytes);
                default:
                    return null;
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
            checksums -> Joiner.on(',').skipNulls().join(transform(checksums, TO_RFC3230_FRAGMENT));

    private Checksums()
    {
        // prevent instantiation
    }

    /**
     * Choose the best checksum algorithm based on the client's stated
     * preferences and what checksums are available.  Ties (e.g., client
     * wants either ADLER32 or MD5 with no preference with both checksums are
     * available) are resolved by a hard-coded ordering of checksum algorithms.
     * The returned value is encoded as a header value for an RFC 3230 Digest
     * header.
     * @param wantDigest The client-supplied Want-Digest header
     * @param attributes The FileAttributes of the targeted file
     * @return the value of a Digest HTTP header, if appropriate.
     */
    public static Optional<String> digestHeader(@Nullable String wantDigest, FileAttributes attributes)
    {
        Optional<Set<Checksum>> knownChecksums = Optional.ofNullable(attributes.getChecksumsIfPresent().orNull());

        Optional<ChecksumType> selected = knownChecksums
                .filter(s -> !s.isEmpty())
                .map(s -> s.stream()
                        .map(Checksum::getType)
                        .collect(Collectors.toCollection(() -> EnumSet.noneOf(ChecksumType.class))))
                .flatMap(t -> Checksums.parseWantDigest(wantDigest, t));

        return knownChecksums
                .filter(s -> selected.isPresent())
                .flatMap(s -> s.stream()
                        .filter(c -> c.getType() == selected.get())
                        .findFirst())
                .map(c -> TO_RFC3230_FRAGMENT.apply(c));
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

        return checksums.values().stream().filter(Objects::nonNull).collect(Collectors.toSet());
    }

    /**
     * Choose the best checksum algorithm based on the client's stated
     * preferences.  Ties (e.g., client wants either ADLER32 or MD5 with no
     * preference) are resolved by a hard-coded ordering of checksum algorithms.
     * @param wantDigest The value of the RFC 3230 Want-Digest HTTP header.
     * @return The best algorithm, if any match.
     */
    public static Optional<ChecksumType> parseWantDigest(String wantDigest)
    {
        return parseWantDigest(wantDigest, EnumSet.allOf(ChecksumType.class));
    }

    /**
     * Choose the best checksum algorithm based on the client's stated
     * preferences and what checksums are available.
     */
    private static Optional<ChecksumType> parseWantDigest(@Nullable String wantDigest,
            EnumSet<ChecksumType> allowedTypes)
    {
        return Optional.ofNullable(wantDigest).flatMap(v ->
                Splitter.on(',').omitEmptyStrings().trimResults().splitToList(v).stream()
                        .map(QualityValue::of)
                        .filter(q -> q.quality() != 0)
                        .filter(q -> ChecksumType.isValid(q.value()))
                        .map(q -> q.mapWith(ChecksumType::getChecksumType))
                        .filter(q -> allowedTypes.contains(q.value()))
                        .sorted(Comparator.<QualityValue<ChecksumType>>comparingDouble(q -> q.quality()).reversed()
                                .thenComparing(q -> q.value(), PREFERRED_CHECKSUM_TYPE_ORDERING))
                        .map(QualityValue::value)
                        .findFirst());
    }

    public static Ordering<Checksum> preferrredOrder()
    {
        return PREFERRED_CHECKSUM_ORDERING;
    }
}
