package org.dcache.util;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.Sets;
import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import org.dcache.vehicles.FileAttributes;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.dcache.util.ChecksumType.*;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertThat;

public class ChecksumsTests
{
    private static final Checksum ADLER32_HELLO_WORLD
            = ChecksumType.ADLER32.calculate("Hello, world".getBytes(StandardCharsets.UTF_8));

    private String _rfc3230;
    private Collection<Checksum> _checksums;

    @Test
    public void shouldGiveEmptyStringForSetOfUnsupportedType()
    {
        givenSet(checksum().ofType(MD4_TYPE).
                withValue("6df23dc03f9b54cc38a0fc1483df6e21"));

        whenGeneratingRfc3230ForSetOfChecksums();

        assertThat(_rfc3230, equalTo(""));
    }

    @Test
    public void shouldGiveEmptyStringForEmptySetOfChecksums()
    {
        givenSet();

        whenGeneratingRfc3230ForSetOfChecksums();

        assertThat(_rfc3230, equalTo(""));
    }

    @Test
    public void shouldGiveCorrectStringForSetOfAdler32()
    {
        givenSet(checksum().ofType(ADLER32).withValue("3da0195"));

        whenGeneratingRfc3230ForSetOfChecksums();

        assertThat(_rfc3230, equalTo("adler32=03da0195"));
    }

    @Test
    public void shouldGiveCorrectStringForSetOfMd5()
    {
        givenSet(checksum().ofType(MD5_TYPE).
                withValue("1d45d92d02ccb88fca6792837093dc38"));

        whenGeneratingRfc3230ForSetOfChecksums();

        assertThat(_rfc3230, equalTo("md5=HUXZLQLMuI/KZ5KDcJPcOA=="));
    }

    @Test
    public void shouldGiveCorrectStringForAdler32AndMD5()
    {
        givenSet(checksum().ofType(ADLER32).withValue("3da0195"),
                checksum().ofType(MD5_TYPE).
                        withValue("1d45d92d02ccb88fca6792837093dc38"));

        whenGeneratingRfc3230ForSetOfChecksums();

        assertThat(_rfc3230, hasOnlyParts("adler32=03da0195",
                "md5=HUXZLQLMuI/KZ5KDcJPcOA=="));
    }

    @Test
    public void shouldGiveCorrectStringForUnsupportedAndAdler32AndMD5()
    {
        givenSet(checksum().ofType(MD4_TYPE).
                        withValue("6df23dc03f9b54cc38a0fc1483df6e21"),
                checksum().ofType(ADLER32).withValue("3da0195"),
                checksum().ofType(MD5_TYPE).
                        withValue("1d45d92d02ccb88fca6792837093dc38"));

        whenGeneratingRfc3230ForSetOfChecksums();

        assertThat(_rfc3230, hasOnlyParts("adler32=03da0195",
                "md5=HUXZLQLMuI/KZ5KDcJPcOA=="));
    }

    @Test
    public void shouldReturnEmptySetDecodingNull()
    {
        Set<Checksum> result = Checksums.decodeRfc3230(null);

        assertThat(result, empty());
    }

    @Test
    public void shouldReturnEmptySetDecodingEmptyString()
    {
        Set<Checksum> result = Checksums.decodeRfc3230("");

        assertThat(result, empty());
    }

    @Test
    public void shouldReturnSingleChecksumForAdler32String()
    {
        Set<Checksum> result = Checksums.decodeRfc3230("adler32=03da0195");

        Set<Checksum> expected = Collections.singleton(new Checksum(ChecksumType.ADLER32, "03da0195"));

        assertThat(result, equalTo(expected));
    }

    @Test
    public void shouldReturnSingleChecksumForAdler32StringWithWhiteSpace()
    {
        Set<Checksum> result = Checksums.decodeRfc3230(" adler32=03da0195 ");

        Set<Checksum> expected = Collections.singleton(new Checksum(ChecksumType.ADLER32, "03da0195"));

        assertThat(result, equalTo(expected));
    }

    @Test
    public void shouldReturnSingleChecksumForCapitalAdler32String()
    {
        Set<Checksum> result = Checksums.decodeRfc3230("ADLER32=03DA0195");

        Set<Checksum> expected = Collections.singleton(new Checksum(ChecksumType.ADLER32, "03DA0195"));

        assertThat(result, equalTo(expected));
    }

    @Test
    public void shouldReturnSingleChecksumForMd5String()
    {
        Set<Checksum> result =
                Checksums.decodeRfc3230("md5=HUXZLQLMuI/KZ5KDcJPcOA==");

        Set<Checksum> expected = Collections.singleton(
                new Checksum(ChecksumType.MD5_TYPE,
                "1d45d92d02ccb88fca6792837093dc38"));

        assertThat(result, equalTo(expected));
    }

    @Test
    public void shouldReturnEmptySetForMalformedMd5String()
    {
        Set<Checksum> result =
                Checksums.decodeRfc3230("md5=THIS-IS-NOT-VALID-DIGEST");

        assertThat(result, empty());
    }

    @Test
    public void shouldReturnSingleChecksumForMd5StringWithSpace()
    {
        Set<Checksum> result =
                Checksums.decodeRfc3230(" md5=HUXZLQLMuI/KZ5KDcJPcOA== ");

        Set<Checksum> expected = Collections.singleton(
                new Checksum(ChecksumType.MD5_TYPE,
                "1d45d92d02ccb88fca6792837093dc38"));

        assertThat(result, equalTo(expected));
    }

    @Test
    public void shouldReturnSingleChecksumForCapitalMd5String()
    {
        Set<Checksum> result =
                Checksums.decodeRfc3230("MD5=HUXZLQLMuI/KZ5KDcJPcOA==");

        Set<Checksum> expected = Collections.singleton(
                new Checksum(ChecksumType.MD5_TYPE,
                "1d45d92d02ccb88fca6792837093dc38"));

        assertThat(result, equalTo(expected));
    }

    @Test
    public void shouldReturnBothForMd5AndAdler32()
    {
        Set<Checksum> result =
                Checksums.decodeRfc3230("adler32=03da0195,md5=HUXZLQLMuI/KZ5KDcJPcOA==");

        Set<Checksum> expected = Sets.newHashSet(newAdler32Checksum("03da0195"),
                newMd5Checksum("1d45d92d02ccb88fca6792837093dc38"));

        assertThat(result, equalTo(expected));
    }

    @Test
    public void shouldReturnBothForAdler32AndMd5()
    {
        Set<Checksum> result =
                Checksums.decodeRfc3230("md5=HUXZLQLMuI/KZ5KDcJPcOA==,adler32=03da0195");

        Set<Checksum> expected = Sets.newHashSet(newAdler32Checksum("03da0195"),
                newMd5Checksum("1d45d92d02ccb88fca6792837093dc38"));

        assertThat(result, equalTo(expected));
    }

    @Test
    public void shouldReturnBothForAdler32AndMd5WithSpaces()
    {
        Set<Checksum> result =
                Checksums.decodeRfc3230("  md5=HUXZLQLMuI/KZ5KDcJPcOA==,adler32=03da0195  ");

        Set<Checksum> expected = Sets.newHashSet(newAdler32Checksum("03da0195"),
                newMd5Checksum("1d45d92d02ccb88fca6792837093dc38"));

        assertThat(result, equalTo(expected));
    }

    @Test
    public void shouldReturnBothForAdler32AndMd5AndUnknown()
    {
        Set<Checksum> result =
                Checksums.decodeRfc3230("md5=HUXZLQLMuI/KZ5KDcJPcOA==,adler32=03da0195,unknown=UNKNOWN-VALUE");

        Set<Checksum> expected = Sets.newHashSet(newAdler32Checksum("03da0195"),
                newMd5Checksum("1d45d92d02ccb88fca6792837093dc38"));

        assertThat(result, equalTo(expected));
    }

    @Test
    public void shouldFindAdler32AsSingleEntry()
    {
        Optional<ChecksumType> type = Checksums.parseWantDigest("adler32");
        assertThat(type.isPresent(), is(equalTo(true)));
        assertThat(type.get(), is(equalTo(ChecksumType.ADLER32)));
    }

    @Test
    public void shouldFindMd5AsSingleEntry()
    {
        Optional<ChecksumType> type = Checksums.parseWantDigest("md5");
        assertThat(type.isPresent(), is(equalTo(true)));
        assertThat(type.get(), is(equalTo(ChecksumType.MD5_TYPE)));
    }

    @Test
    public void shouldFindSingleGoodEntryWithQ()
    {
        Optional<ChecksumType> type = Checksums.parseWantDigest("adler32;q=0.5");
        assertThat(type.isPresent(), is(equalTo(true)));
        assertThat(type.get(), is(equalTo(ChecksumType.ADLER32)));
    }

    @Test
    public void shouldSelectSecondAsBestByInternalPreference()
    {
        Optional<ChecksumType> type = Checksums.parseWantDigest("adler32,md5");
        assertThat(type.isPresent(), is(equalTo(true)));
        assertThat(type.get(), is(equalTo(ChecksumType.MD5_TYPE)));
    }

    @Test
    public void shouldSelectFirstAsBestByInternalPreference()
    {
        Optional<ChecksumType> type = Checksums.parseWantDigest("md5,adler32");
        assertThat(type.isPresent(), is(equalTo(true)));
        assertThat(type.get(), is(equalTo(ChecksumType.MD5_TYPE)));
    }

    @Test
    public void shouldSelectBestByExplicitQ()
    {
        Optional<ChecksumType> type = Checksums.parseWantDigest("adler32;q=0.5,md5;q=1");
        assertThat(type.isPresent(), is(equalTo(true)));
        assertThat(type.get(), is(equalTo(ChecksumType.MD5_TYPE)));
    }

    @Test
    public void shouldSelectBestByImplicitQ()
    {
        Optional<ChecksumType> type = Checksums.parseWantDigest("adler32;q=0.5,md5");
        assertThat(type.isPresent(), is(equalTo(true)));
        assertThat(type.get(), is(equalTo(ChecksumType.MD5_TYPE)));
    }

    @Test
    public void shouldIgnoreUnknownAlgorithm()
    {
        Optional<ChecksumType> type = Checksums.parseWantDigest("adler32;q=0.5,UNKNOWN;q=1");
        assertThat(type.isPresent(), is(equalTo(true)));
        assertThat(type.get(), is(equalTo(ChecksumType.ADLER32)));
    }

    @Test
    public void shouldGenerateNoHeaderIfNoWantDigest()
    {
        Optional<String> value = Checksums.digestHeader((String)null, FileAttributes.ofChecksum(ADLER32_HELLO_WORLD));
        assertThat(value.isPresent(), is(equalTo(false)));
    }

    @Test
    public void shouldGenerateNHeaderIfWantDigestOfAvailableChecksum()
    {
        Optional<String> value = Checksums.digestHeader("adler32", FileAttributes.ofChecksum(ADLER32_HELLO_WORLD));
        assertThat(value.isPresent(), is(equalTo(true)));
        assertThat(value.get(), startsWith("adler32="));
    }

    @Test
    public void shouldGenerateNoHeaderIfWantDigestOfUnavailableChecksum()
    {
        Optional<String> value = Checksums.digestHeader("md5", FileAttributes.ofChecksum(ADLER32_HELLO_WORLD));
        assertThat(value.isPresent(), is(equalTo(false)));
    }

    @Test
    public void shouldGenerateNoHeaderIfWantDigestButNoChecksumAvailable()
    {
        Optional<String> value = Checksums.digestHeader("adler32", new FileAttributes());
        assertThat(value.isPresent(), is(equalTo(false)));
    }


    private Checksum newMd5Checksum(String value)
    {
        return new Checksum(ChecksumType.MD5_TYPE, value);
    }

    private Checksum newAdler32Checksum(String value)
    {
        return new Checksum(ChecksumType.ADLER32, value);
    }

    private void givenSet(ChecksumBuilder... builders)
    {
        _checksums = new HashSet<>();

        for(ChecksumBuilder builder : builders) {
            _checksums.add(builder.build());
        }
    }

    private void whenGeneratingRfc3230ForSetOfChecksums()
    {
        _rfc3230 = Checksums.TO_RFC3230.apply(_checksums);
    }

    private ChecksumBuilder checksum()
    {
        return new ChecksumBuilder();
    }

    private class ChecksumBuilder
    {
        private ChecksumType _type;
        private String _value;

        public ChecksumBuilder ofType(ChecksumType type)
        {
            checkNotNull(type);
            _type = type;
            return this;
        }

        public ChecksumBuilder withValue(String value)
        {
            checkNotNull(value);
            _value = value;
            return this;
        }

        public Checksum build()
        {
            checkNotNull(_value);
            checkNotNull(_type);
            return new Checksum(_type, _value);
        }
    }

    private static HasOnlyParts hasOnlyParts(String... parts)
    {
        return new HasOnlyParts(parts);
    }

    /**
     * Matcher that passes if the supplied comma-separated list of parts
     * contains all of the matching parts and nothing else.  The order of
     * the parts does not matter.
     */
    private static class HasOnlyParts extends TypeSafeMatcher<String>
    {
        private Set<String> _needles = new HashSet<>();
        private String _missing;
        private String _extra;

        public HasOnlyParts(String... parts)
        {
            _needles.addAll(Arrays.asList(parts));
        }

        @Override
        protected boolean matchesSafely(String t)
        {
            Set<String> haystack = Sets.newHashSet(Splitter.on(',').split(t));

            if(!haystack.containsAll(_needles)) {
                _needles.removeAll(haystack);
                _missing = Joiner.on(", ").join(_needles);
                return false;
            }

            if(!_needles.containsAll(haystack)) {
                haystack.removeAll(_needles);
                _extra = Joiner.on(", ").join(haystack);
                return false;
            }

            return true;
        }

        @Override
        public void describeTo(Description d)
        {
            if(_missing != null) {
                d.appendText("missing: ").appendValue(_missing);
            } else {
                d.appendText("unexpected: ").appendValue(_extra);
            }
        }

    }
}
