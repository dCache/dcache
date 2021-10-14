package org.dcache.util;

import static java.util.Arrays.asList;
import static java.util.Objects.requireNonNull;
import static org.dcache.util.ChecksumType.ADLER32;
import static org.dcache.util.ChecksumType.MD4_TYPE;
import static org.dcache.util.ChecksumType.MD5_TYPE;
import static org.dcache.util.ChecksumType.SHA1;
import static org.dcache.util.ChecksumType.SHA256;
import static org.dcache.util.ChecksumType.SHA512;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.Sets;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.dcache.vehicles.FileAttributes;
import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;
import org.junit.Test;

public class ChecksumsTests {

    private static final Checksum ADLER32_HELLO_WORLD
          = ChecksumType.ADLER32.calculate("Hello, world".getBytes(StandardCharsets.UTF_8));

    private String _rfc3230;
    private Collection<Checksum> _checksums;

    @Test
    public void shouldGiveEmptyStringForSetOfUnsupportedType() {
        givenSet(checksum().ofType(MD4_TYPE).
              withValue("6df23dc03f9b54cc38a0fc1483df6e21"));

        whenGeneratingRfc3230ForSetOfChecksums();

        assertThat(_rfc3230, equalTo(""));
    }

    @Test
    public void shouldGiveEmptyStringForEmptySetOfChecksums() {
        givenSet();

        whenGeneratingRfc3230ForSetOfChecksums();

        assertThat(_rfc3230, equalTo(""));
    }

    @Test
    public void shouldGiveCorrectStringForSetOfAdler32() {
        givenSet(checksum().ofType(ADLER32).withValue("3da0195"));

        whenGeneratingRfc3230ForSetOfChecksums();

        assertThat(_rfc3230, equalTo("adler32=03da0195"));
    }

    @Test
    public void shouldGiveCorrectStringForSetOfMd5() {
        givenSet(checksum().ofType(MD5_TYPE).
              withValue("1d45d92d02ccb88fca6792837093dc38"));

        whenGeneratingRfc3230ForSetOfChecksums();

        assertThat(_rfc3230, equalTo("md5=HUXZLQLMuI/KZ5KDcJPcOA=="));
    }

    @Test
    public void shouldGiveCorrectStringForSetOfSha1() {
        givenSet(checksum().ofType(SHA1).
              withValue("e7e46ff10df57532f816596327e98f9597b8c21e"));

        whenGeneratingRfc3230ForSetOfChecksums();

        assertThat(_rfc3230, equalTo("sha=5+Rv8Q31dTL4FlljJ+mPlZe4wh4="));
    }

    @Test
    public void shouldGiveCorrectStringForSetOfSha256() {
        givenSet(checksum().ofType(SHA256).
              withValue("36eb9dc4a150f3b2e7790991e5f978f887632f92a2002b7c55340c2a9ef1c31c"));

        whenGeneratingRfc3230ForSetOfChecksums();

        assertThat(_rfc3230, equalTo("sha-256=NuudxKFQ87LneQmR5fl4+IdjL5KiACt8VTQMKp7xwxw="));
    }

    @Test
    public void shouldGiveCorrectStringForSetOfSha512() {
        givenSet(checksum().ofType(SHA512).
              withValue(
                    "07e547d9586f6a73f73fbac0435ed76951218fb7d0c8d788a309d785436bbb642e93a252a954f23912547d1e8a3b5ed6e1bfd7097821233fa0538f3db854fee6"));

        whenGeneratingRfc3230ForSetOfChecksums();

        assertThat(_rfc3230, equalTo(
              "sha-512=B+VH2VhvanP3P7rAQ17XaVEhj7fQyNeIownXhUNru2Quk6JSqVTyORJUfR6KO17W4b/XCXghIz+gU489uFT+5g=="));
    }

    @Test
    public void shouldGiveCorrectStringForAdler32AndMD5() {
        givenSet(checksum().ofType(ADLER32).withValue("3da0195"),
              checksum().ofType(MD5_TYPE).
                    withValue("1d45d92d02ccb88fca6792837093dc38"));

        whenGeneratingRfc3230ForSetOfChecksums();

        assertThat(_rfc3230, hasOnlyParts("adler32=03da0195",
              "md5=HUXZLQLMuI/KZ5KDcJPcOA=="));
    }

    @Test
    public void shouldGiveCorrectStringForUnsupportedAndAdler32AndMD5() {
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
    public void shouldGiveCorrectStringForUnsupportedAndAdler32AndMD5andSha1andSha256andSha512() {
        givenSet(checksum().ofType(MD4_TYPE).withValue("6df23dc03f9b54cc38a0fc1483df6e21"),
              checksum().ofType(ADLER32).withValue("3da0195"),
              checksum().ofType(MD5_TYPE).withValue("1d45d92d02ccb88fca6792837093dc38"),
              checksum().ofType(SHA256)
                    .withValue("36eb9dc4a150f3b2e7790991e5f978f887632f92a2002b7c55340c2a9ef1c31c"),
              checksum().ofType(SHA1).withValue("e7e46ff10df57532f816596327e98f9597b8c21e"),
              checksum().ofType(SHA512).withValue(
                    "07e547d9586f6a73f73fbac0435ed76951218fb7d0c8d788a309d785436bbb642e93a252a954f23912547d1e8a3b5ed6e1bfd7097821233fa0538f3db854fee6")
        );

        whenGeneratingRfc3230ForSetOfChecksums();

        assertThat(_rfc3230, hasOnlyParts("adler32=03da0195",
              "md5=HUXZLQLMuI/KZ5KDcJPcOA==",
              "sha-256=NuudxKFQ87LneQmR5fl4+IdjL5KiACt8VTQMKp7xwxw=",
              "sha-512=B+VH2VhvanP3P7rAQ17XaVEhj7fQyNeIownXhUNru2Quk6JSqVTyORJUfR6KO17W4b/XCXghIz+gU489uFT+5g==",
              "sha=5+Rv8Q31dTL4FlljJ+mPlZe4wh4="));
    }

    @Test
    public void shouldReturnEmptySetDecodingNull() {
        Set<Checksum> result = Checksums.decodeRfc3230(null);

        assertThat(result, empty());
    }

    @Test
    public void shouldReturnEmptySetDecodingEmptyString() {
        Set<Checksum> result = Checksums.decodeRfc3230("");

        assertThat(result, empty());
    }

    @Test
    public void shouldReturnSingleChecksumForAdler32String() {
        Set<Checksum> result = Checksums.decodeRfc3230("adler32=03da0195");

        Set<Checksum> expected = Collections.singleton(
              new Checksum(ChecksumType.ADLER32, "03da0195"));

        assertThat(result, equalTo(expected));
    }

    @Test
    public void shouldReturnSingleChecksumForAdler32StringWithWhiteSpace() {
        Set<Checksum> result = Checksums.decodeRfc3230(" adler32=03da0195 ");

        Set<Checksum> expected = Collections.singleton(
              new Checksum(ChecksumType.ADLER32, "03da0195"));

        assertThat(result, equalTo(expected));
    }

    @Test
    public void shouldReturnSingleChecksumForCapitalAdler32String() {
        Set<Checksum> result = Checksums.decodeRfc3230("ADLER32=03DA0195");

        Set<Checksum> expected = Collections.singleton(
              new Checksum(ChecksumType.ADLER32, "03DA0195"));

        assertThat(result, equalTo(expected));
    }

    @Test
    public void shouldReturnSingleChecksumForMd5String() {
        Set<Checksum> result =
              Checksums.decodeRfc3230("md5=HUXZLQLMuI/KZ5KDcJPcOA==");

        Set<Checksum> expected = Collections.singleton(
              new Checksum(ChecksumType.MD5_TYPE,
                    "1d45d92d02ccb88fca6792837093dc38"));

        assertThat(result, equalTo(expected));
    }

    @Test
    public void shouldReturnEmptySetForMalformedMd5String() {
        Set<Checksum> result =
              Checksums.decodeRfc3230("md5=THIS-IS-NOT-VALID-DIGEST");

        assertThat(result, empty());
    }

    @Test
    public void shouldReturnSingleChecksumForMd5StringWithSpace() {
        Set<Checksum> result =
              Checksums.decodeRfc3230(" md5=HUXZLQLMuI/KZ5KDcJPcOA== ");

        Set<Checksum> expected = Collections.singleton(
              new Checksum(ChecksumType.MD5_TYPE,
                    "1d45d92d02ccb88fca6792837093dc38"));

        assertThat(result, equalTo(expected));
    }

    @Test
    public void shouldReturnSingleChecksumForCapitalMd5String() {
        Set<Checksum> result =
              Checksums.decodeRfc3230("MD5=HUXZLQLMuI/KZ5KDcJPcOA==");

        Set<Checksum> expected = Collections.singleton(
              new Checksum(ChecksumType.MD5_TYPE,
                    "1d45d92d02ccb88fca6792837093dc38"));

        assertThat(result, equalTo(expected));
    }

    @Test
    public void shouldReturnSingleChecksumForSha1String() {
        Set<Checksum> result = Checksums.decodeRfc3230("sha=5+Rv8Q31dTL4FlljJ+mPlZe4wh4=");

        Set<Checksum> expected = Collections.singleton(
              new Checksum(SHA1, "e7e46ff10df57532f816596327e98f9597b8c21e"));

        assertThat(result, equalTo(expected));
    }

    @Test
    public void shouldReturnSingleChecksumForSha1StringWithSpace() {
        Set<Checksum> result = Checksums.decodeRfc3230(" sha=5+Rv8Q31dTL4FlljJ+mPlZe4wh4= ");

        Set<Checksum> expected = Collections.singleton(
              new Checksum(SHA1, "e7e46ff10df57532f816596327e98f9597b8c21e"));

        assertThat(result, equalTo(expected));
    }

    @Test
    public void shouldReturnSingleChecksumForCapitalSha1String() {
        Set<Checksum> result = Checksums.decodeRfc3230("SHA=5+Rv8Q31dTL4FlljJ+mPlZe4wh4=");

        Set<Checksum> expected = Collections.singleton(
              new Checksum(SHA1, "e7e46ff10df57532f816596327e98f9597b8c21e"));

        assertThat(result, equalTo(expected));
    }

    @Test
    public void shouldReturnSingleChecksumForMalformedSha1String() {
        Set<Checksum> result = Checksums.decodeRfc3230("sha=THIS-IS-NOT-VALID-DIGEST");

        assertThat(result, empty());
    }

    @Test
    public void shouldReturnSingleChecksumForSha256String() {
        Set<Checksum> result = Checksums.decodeRfc3230(
              "sha-256=NuudxKFQ87LneQmR5fl4+IdjL5KiACt8VTQMKp7xwxw=");

        Set<Checksum> expected = Collections.singleton(new Checksum(SHA256,
              "36eb9dc4a150f3b2e7790991e5f978f887632f92a2002b7c55340c2a9ef1c31c"));

        assertThat(result, equalTo(expected));
    }

    @Test
    public void shouldReturnSingleChecksumForSha256StringWithSpace() {
        Set<Checksum> result = Checksums.decodeRfc3230(
              " sha-256=NuudxKFQ87LneQmR5fl4+IdjL5KiACt8VTQMKp7xwxw= ");

        Set<Checksum> expected = Collections.singleton(new Checksum(SHA256,
              "36eb9dc4a150f3b2e7790991e5f978f887632f92a2002b7c55340c2a9ef1c31c"));

        assertThat(result, equalTo(expected));
    }

    @Test
    public void shouldReturnSingleChecksumForCapitalSha256String() {
        Set<Checksum> result = Checksums.decodeRfc3230(
              "SHA-256=NuudxKFQ87LneQmR5fl4+IdjL5KiACt8VTQMKp7xwxw=");

        Set<Checksum> expected = Collections.singleton(new Checksum(SHA256,
              "36eb9dc4a150f3b2e7790991e5f978f887632f92a2002b7c55340c2a9ef1c31c"));

        assertThat(result, equalTo(expected));
    }

    @Test
    public void shouldReturnSingleChecksumForMalformedSha256String() {
        Set<Checksum> result = Checksums.decodeRfc3230("sha-256=THIS-IS-NOT-VALID-DIGEST");

        assertThat(result, empty());
    }

    @Test
    public void shouldReturnSingleChecksumForSha512String() {
        Set<Checksum> result = Checksums.decodeRfc3230(
              "sha-512=B+VH2VhvanP3P7rAQ17XaVEhj7fQyNeIownXhUNru2Quk6JSqVTyORJUfR6KO17W4b/XCXghIz+gU489uFT+5g==");

        Set<Checksum> expected = Collections.singleton(new Checksum(SHA512,
              "07e547d9586f6a73f73fbac0435ed76951218fb7d0c8d788a309d785436bbb642e93a252a954f23912547d1e8a3b5ed6e1bfd7097821233fa0538f3db854fee6"));

        assertThat(result, equalTo(expected));
    }

    @Test
    public void shouldReturnSingleChecksumForSha512StringWithSpace() {
        Set<Checksum> result = Checksums.decodeRfc3230(
              " sha-512=B+VH2VhvanP3P7rAQ17XaVEhj7fQyNeIownXhUNru2Quk6JSqVTyORJUfR6KO17W4b/XCXghIz+gU489uFT+5g== ");

        Set<Checksum> expected = Collections.singleton(new Checksum(SHA512,
              "07e547d9586f6a73f73fbac0435ed76951218fb7d0c8d788a309d785436bbb642e93a252a954f23912547d1e8a3b5ed6e1bfd7097821233fa0538f3db854fee6"));

        assertThat(result, equalTo(expected));
    }

    @Test
    public void shouldReturnSingleChecksumForCapitalSha512String() {
        Set<Checksum> result = Checksums.decodeRfc3230(
              "SHA-512=B+VH2VhvanP3P7rAQ17XaVEhj7fQyNeIownXhUNru2Quk6JSqVTyORJUfR6KO17W4b/XCXghIz+gU489uFT+5g==");

        Set<Checksum> expected = Collections.singleton(new Checksum(SHA512,
              "07e547d9586f6a73f73fbac0435ed76951218fb7d0c8d788a309d785436bbb642e93a252a954f23912547d1e8a3b5ed6e1bfd7097821233fa0538f3db854fee6"));

        assertThat(result, equalTo(expected));
    }

    @Test
    public void shouldReturnSingleChecksumForMalformedSha512String() {
        Set<Checksum> result = Checksums.decodeRfc3230("sha-512=THIS-IS-NOT-VALID-DIGEST");

        assertThat(result, empty());
    }

    @Test
    public void shouldReturnBothForMd5AndAdler32() {
        Set<Checksum> result =
              Checksums.decodeRfc3230("adler32=03da0195,md5=HUXZLQLMuI/KZ5KDcJPcOA==");

        Set<Checksum> expected = Sets.newHashSet(newAdler32Checksum("03da0195"),
              newMd5Checksum("1d45d92d02ccb88fca6792837093dc38"));

        assertThat(result, equalTo(expected));
    }

    @Test
    public void shouldReturnBothForAdler32AndMd5() {
        Set<Checksum> result =
              Checksums.decodeRfc3230("md5=HUXZLQLMuI/KZ5KDcJPcOA==,adler32=03da0195");

        Set<Checksum> expected = Sets.newHashSet(newAdler32Checksum("03da0195"),
              newMd5Checksum("1d45d92d02ccb88fca6792837093dc38"));

        assertThat(result, equalTo(expected));
    }

    @Test
    public void shouldReturnBothForAdler32AndMd5WithSpaces() {
        Set<Checksum> result =
              Checksums.decodeRfc3230("  md5=HUXZLQLMuI/KZ5KDcJPcOA==,adler32=03da0195  ");

        Set<Checksum> expected = Sets.newHashSet(newAdler32Checksum("03da0195"),
              newMd5Checksum("1d45d92d02ccb88fca6792837093dc38"));

        assertThat(result, equalTo(expected));
    }

    @Test
    public void shouldReturnBothForAdler32AndMd5AndUnknown() {
        Set<Checksum> result =
              Checksums.decodeRfc3230(
                    "md5=HUXZLQLMuI/KZ5KDcJPcOA==,adler32=03da0195,unknown=UNKNOWN-VALUE");

        Set<Checksum> expected = Sets.newHashSet(newAdler32Checksum("03da0195"),
              newMd5Checksum("1d45d92d02ccb88fca6792837093dc38"));

        assertThat(result, equalTo(expected));
    }

    @Test
    public void shouldFindAdler32AsSingleEntry() {
        Optional<ChecksumType> type = Checksums.parseWantDigest("adler32");
        assertThat(type.isPresent(), is(equalTo(true)));
        assertThat(type.get(), is(equalTo(ChecksumType.ADLER32)));
    }

    @Test
    public void shouldFindMd5AsSingleEntry() {
        Optional<ChecksumType> type = Checksums.parseWantDigest("md5");
        assertThat(type.isPresent(), is(equalTo(true)));
        assertThat(type.get(), is(equalTo(ChecksumType.MD5_TYPE)));
    }

    @Test
    public void shouldFindSha1AsSingleEntry() {
        Optional<ChecksumType> type = Checksums.parseWantDigest("sha-1");
        assertThat(type.isPresent(), is(equalTo(true)));
        assertThat(type.get(), is(equalTo(SHA1)));
    }

    @Test
    public void shouldFindSha256AsSingleEntry() {
        Optional<ChecksumType> type = Checksums.parseWantDigest("sha-256");
        assertThat(type.isPresent(), is(equalTo(true)));
        assertThat(type.get(), is(equalTo(SHA256)));
    }

    @Test
    public void shouldFindSha512AsSingleEntry() {
        Optional<ChecksumType> type = Checksums.parseWantDigest("sha-512");
        assertThat(type.isPresent(), is(equalTo(true)));
        assertThat(type.get(), is(equalTo(SHA512)));
    }

    @Test
    public void shouldFindSingleGoodEntryWithQ() {
        Optional<ChecksumType> type = Checksums.parseWantDigest("adler32;q=0.5");
        assertThat(type.isPresent(), is(equalTo(true)));
        assertThat(type.get(), is(equalTo(ChecksumType.ADLER32)));
    }

    @Test
    public void shouldSelectSecondAsBestByInternalPreference() {
        Optional<ChecksumType> type = Checksums.parseWantDigest("adler32,md5");
        assertThat(type.isPresent(), is(equalTo(true)));
        assertThat(type.get(), is(equalTo(ChecksumType.MD5_TYPE)));
    }

    @Test
    public void shouldSelectFirstAsBestByInternalPreference() {
        Optional<ChecksumType> type = Checksums.parseWantDigest("md5,adler32");
        assertThat(type.isPresent(), is(equalTo(true)));
        assertThat(type.get(), is(equalTo(ChecksumType.MD5_TYPE)));
    }

    @Test
    public void shouldSelectBestByExplicitQ() {
        Optional<ChecksumType> type = Checksums.parseWantDigest("adler32;q=0.5,md5;q=1");
        assertThat(type.isPresent(), is(equalTo(true)));
        assertThat(type.get(), is(equalTo(ChecksumType.MD5_TYPE)));
    }

    @Test
    public void shouldSelectBestByImplicitQ() {
        Optional<ChecksumType> type = Checksums.parseWantDigest("adler32;q=0.5,md5");
        assertThat(type.isPresent(), is(equalTo(true)));
        assertThat(type.get(), is(equalTo(ChecksumType.MD5_TYPE)));
    }

    @Test
    public void shouldIgnoreUnknownAlgorithm() {
        Optional<ChecksumType> type = Checksums.parseWantDigest("adler32;q=0.5,UNKNOWN;q=1");
        assertThat(type.isPresent(), is(equalTo(true)));
        assertThat(type.get(), is(equalTo(ChecksumType.ADLER32)));
    }

    @Test
    public void shouldGenerateNoHeaderIfNoWantDigest() {
        Optional<String> value = Checksums.digestHeader((String) null,
              FileAttributes.ofChecksum(ADLER32_HELLO_WORLD));
        assertThat(value.isPresent(), is(equalTo(false)));
    }

    @Test
    public void shouldGenerateNHeaderIfWantDigestOfAvailableChecksum() {
        Optional<String> value = Checksums.digestHeader("adler32",
              FileAttributes.ofChecksum(ADLER32_HELLO_WORLD));
        assertThat(value.isPresent(), is(equalTo(true)));
        assertThat(value.get(), startsWith("adler32="));
    }

    @Test
    public void shouldGenerateNoHeaderIfWantDigestOfUnavailableChecksum() {
        Optional<String> value = Checksums.digestHeader("md5",
              FileAttributes.ofChecksum(ADLER32_HELLO_WORLD));
        assertThat(value.isPresent(), is(equalTo(false)));
    }

    @Test
    public void shouldGenerateNoHeaderIfWantDigestButNoChecksumAvailable() {
        Optional<String> value = Checksums.digestHeader("adler32", new FileAttributes());
        assertThat(value.isPresent(), is(equalTo(false)));
    }

    @Test
    public void shouldReturnEmptyWantDigestForEmptyChecksums() {
        Set<Checksum> checksums = Collections.emptySet();

        Optional<String> wantDigest = Checksums.asWantDigest(checksums);

        assertFalse(wantDigest.isPresent());
    }

    @Test
    public void shouldReturnEmptyWantDigestForNonMappedChecksum() {
        Checksum checksum = newMd4Checksum("12345678901234567890123456789012");
        Set<Checksum> checksums = Collections.singleton(checksum);

        Optional<String> wantDigest = Checksums.asWantDigest(checksums);

        assertFalse(wantDigest.isPresent());
    }

    @Test
    public void shouldReturnMd5WantDigestForSingleMd5Checksum() {
        Checksum checksum = newMd5Checksum("12345678901234567890123456789012");
        Set<Checksum> checksums = Collections.singleton(checksum);

        Optional<String> wantDigest = Checksums.asWantDigest(checksums);

        assertTrue(wantDigest.isPresent());
        assertThat(wantDigest.get(), is(equalTo("md5")));
    }

    @Test
    public void shouldReturnAdler32WantDigestForSingleAdler32Checksum() {
        Checksum checksum = newAdler32Checksum("03da0195");
        Set<Checksum> checksums = Collections.singleton(checksum);

        Optional<String> wantDigest = Checksums.asWantDigest(checksums);

        assertTrue(wantDigest.isPresent());
        assertThat(wantDigest.get(), is(equalTo("adler32")));
    }

    @Test
    public void shouldReturnWantDigestForSingleSha1Checksum() {
        Checksum checksum = newSha1Checksum("e7e46ff10df57532f816596327e98f9597b8c21e");
        Set<Checksum> checksums = Collections.singleton(checksum);

        Optional<String> wantDigest = Checksums.asWantDigest(checksums);

        assertTrue(wantDigest.isPresent());
        assertThat(wantDigest.get(), is(equalTo("sha")));
    }

    @Test
    public void shouldReturnWantDigestForSingleSha256Checksum() {
        Checksum checksum = newSha256Checksum(
              "36eb9dc4a150f3b2e7790991e5f978f887632f92a2002b7c55340c2a9ef1c31c");
        Set<Checksum> checksums = Collections.singleton(checksum);

        Optional<String> wantDigest = Checksums.asWantDigest(checksums);

        assertTrue(wantDigest.isPresent());
        assertThat(wantDigest.get(), is(equalTo("sha-256")));
    }

    @Test
    public void shouldReturnWantDigestForSingleSha512Checksum() {
        Checksum checksum = newSha512Checksum(
              "07e547d9586f6a73f73fbac0435ed76951218fb7d0c8d788a309d785436bbb642e93a252a954f23912547d1e8a3b5ed6e1bfd7097821233fa0538f3db854fee6");
        Set<Checksum> checksums = Collections.singleton(checksum);

        Optional<String> wantDigest = Checksums.asWantDigest(checksums);

        assertTrue(wantDigest.isPresent());
        assertThat(wantDigest.get(), is(equalTo("sha-512")));
    }

    @Test
    public void shouldReturnMd5Adler32WantDigestForAdler32Md5Checksum() {
        List<Checksum> checksums = asList(
              newAdler32Checksum("03da0195"),
              newMd5Checksum("12345678901234567890123456789012")
        );

        Optional<String> wantDigest = Checksums.asWantDigest(checksums);

        assertTrue(wantDigest.isPresent());
        assertThat(wantDigest.get(), is(equalTo("md5,adler32;q=0.5")));
    }

    @Test
    public void shouldReturnMd5Adler32WantDigestForMd5Adler32Checksum() {
        List<Checksum> checksums = asList(
              newMd5Checksum("12345678901234567890123456789012"),
              newAdler32Checksum("03da0195")
        );

        Optional<String> wantDigest = Checksums.asWantDigest(checksums);

        assertTrue(wantDigest.isPresent());
        assertThat(wantDigest.get(), is(equalTo("md5,adler32;q=0.5")));
    }

    @Test
    public void shouldReturnMd5Adler32WantDigestForMd5Md4Adler32Checksum() {
        List<Checksum> checksums = asList(
              newMd5Checksum("12345678901234567890123456789012"),
              newMd4Checksum("12345678901234567890123456789012"),
              newAdler32Checksum("03da0195")
        );

        Optional<String> wantDigest = Checksums.asWantDigest(checksums);

        assertTrue(wantDigest.isPresent());
        assertThat(wantDigest.get(), is(equalTo("md5,adler32;q=0.5")));
    }

    @Test
    public void shouldReturnWantDigestForSha1Sha256sHA512Checksum() {
        List<Checksum> checksums = asList(
              newSha1Checksum("e7e46ff10df57532f816596327e98f9597b8c21e"),
              newSha256Checksum("36eb9dc4a150f3b2e7790991e5f978f887632f92a2002b7c55340c2a9ef1c31c"),
              newSha512Checksum(
                    "07e547d9586f6a73f73fbac0435ed76951218fb7d0c8d788a309d785436bbb642e93a252a954f23912547d1e8a3b5ed6e1bfd7097821233fa0538f3db854fee6")
        );

        Optional<String> wantDigest = Checksums.asWantDigest(checksums);

        assertTrue(wantDigest.isPresent());
        assertThat(wantDigest.get(), is(equalTo("sha-512,sha-256;q=0.7,sha;q=0.3")));
    }

    @Test
    public void shouldBuildExpectedGenericWantDigest() {
        String wantDigest = Checksums.buildGenericWantDigest();

        assertThat(wantDigest,
              is(equalTo("sha-512,sha-256;q=0.8,sha;q=0.6,md5;q=0.4,adler32;q=0.2")));
    }

    private Checksum newMd4Checksum(String value) {
        return new Checksum(ChecksumType.MD4_TYPE, value);
    }

    private Checksum newMd5Checksum(String value) {
        return new Checksum(ChecksumType.MD5_TYPE, value);
    }

    private Checksum newAdler32Checksum(String value) {
        return new Checksum(ChecksumType.ADLER32, value);
    }

    private Checksum newSha1Checksum(String value) {
        return new Checksum(SHA1, value);
    }

    private Checksum newSha256Checksum(String value) {
        return new Checksum(SHA256, value);
    }

    private Checksum newSha512Checksum(String value) {
        return new Checksum(SHA512, value);
    }

    private void givenSet(ChecksumBuilder... builders) {
        _checksums = new HashSet<>();

        for (ChecksumBuilder builder : builders) {
            _checksums.add(builder.build());
        }
    }

    private void whenGeneratingRfc3230ForSetOfChecksums() {
        _rfc3230 = Checksums.TO_RFC3230.apply(_checksums);
    }

    private ChecksumBuilder checksum() {
        return new ChecksumBuilder();
    }

    private class ChecksumBuilder {

        private ChecksumType _type;
        private String _value;

        public ChecksumBuilder ofType(ChecksumType type) {
            requireNonNull(type);
            _type = type;
            return this;
        }

        public ChecksumBuilder withValue(String value) {
            requireNonNull(value);
            _value = value;
            return this;
        }

        public Checksum build() {
            requireNonNull(_value);
            requireNonNull(_type);
            return new Checksum(_type, _value);
        }
    }

    private static HasOnlyParts hasOnlyParts(String... parts) {
        return new HasOnlyParts(parts);
    }

    /**
     * Matcher that passes if the supplied comma-separated list of parts contains all of the
     * matching parts and nothing else.  The order of the parts does not matter.
     */
    private static class HasOnlyParts extends TypeSafeMatcher<String> {

        private Set<String> _needles = new HashSet<>();
        private String _missing;
        private String _extra;

        public HasOnlyParts(String... parts) {
            _needles.addAll(Arrays.asList(parts));
        }

        @Override
        protected boolean matchesSafely(String t) {
            Set<String> haystack = Sets.newHashSet(Splitter.on(',').split(t));

            if (!haystack.containsAll(_needles)) {
                _needles.removeAll(haystack);
                _missing = Joiner.on(", ").join(_needles);
                return false;
            }

            if (!_needles.containsAll(haystack)) {
                haystack.removeAll(_needles);
                _extra = Joiner.on(", ").join(haystack);
                return false;
            }

            return true;
        }

        @Override
        public void describeTo(Description d) {
            if (_missing != null) {
                d.appendText("missing: ").appendValue(_missing);
            } else {
                d.appendText("unexpected: ").appendValue(_extra);
            }
        }

    }
}
