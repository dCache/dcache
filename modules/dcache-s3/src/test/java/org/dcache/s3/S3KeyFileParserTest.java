package org.dcache.s3;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThrows;

import java.util.Map;
import org.dcache.util.files.LineBasedParser.UnrecoverableParsingException;
import org.junit.Before;
import org.junit.Test;

public class S3KeyFileParserTest {

    private S3KeyFileParser parser;

    @Before
    public void setup() {
        parser = new S3KeyFileParser();
    }

    @Test
    public void shouldParseKeyAndSecret() throws Exception {
        parser.accept("AKIAEXAMPLE wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY");

        Map<String, String> keys = parser.build();

        assertThat(keys, hasEntry("AKIAEXAMPLE", "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY"));
    }

    @Test
    public void shouldAcceptTabsAndExtraWhitespace() throws Exception {
        parser.accept("  AKIAEXAMPLE \t secret  ");

        assertThat(parser.build(), hasEntry("AKIAEXAMPLE", "secret"));
    }

    @Test
    public void shouldIgnoreCommentsAndEmptyLines() throws Exception {
        parser.accept("# access key for alice");
        parser.accept("");
        parser.accept("   ");
        parser.accept("AKIAEXAMPLE secret");

        assertThat(parser.build(), aMapWithSize(1));
    }

    @Test
    public void shouldKeepHashInSecret() throws Exception {
        parser.accept("AKIAEXAMPLE sec#ret");

        assertThat(parser.build(), hasEntry("AKIAEXAMPLE", "sec#ret"));
    }

    @Test
    public void shouldParseMultipleKeys() throws Exception {
        parser.accept("AKIAALICE secret-1");
        parser.accept("AKIABOB secret-2");

        Map<String, String> keys = parser.build();

        assertThat(keys, hasEntry("AKIAALICE", "secret-1"));
        assertThat(keys, hasEntry("AKIABOB", "secret-2"));
    }

    @Test
    public void shouldRejectLineWithoutSecret() {
        assertThrows(UnrecoverableParsingException.class, () -> parser.accept("AKIAEXAMPLE"));
    }

    @Test
    public void shouldRejectLineWithExtraFieldWithoutLeakingSecret() {
        UnrecoverableParsingException e = assertThrows(UnrecoverableParsingException.class,
              () -> parser.accept("AKIAEXAMPLE top-secret extra"));

        assertThat(e.getMessage(), not(containsString("top-secret")));
    }

    @Test
    public void shouldRejectDuplicateAccessKey() throws Exception {
        parser.accept("AKIAEXAMPLE secret-1");

        UnrecoverableParsingException e = assertThrows(UnrecoverableParsingException.class,
              () -> parser.accept("AKIAEXAMPLE secret-2"));

        assertThat(e.getMessage(), containsString("AKIAEXAMPLE"));
        assertThat(e.getMessage(), not(containsString("secret-2")));
    }
}
