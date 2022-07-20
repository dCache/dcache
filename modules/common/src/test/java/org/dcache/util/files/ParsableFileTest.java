/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2021-2022 Deutsches Elektronen-Synchrotron
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.dcache.util.files;

import static com.google.common.base.Preconditions.checkState;
import static java.time.temporal.ChronoUnit.SECONDS;
import static java.util.Objects.requireNonNull;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.theInstance;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;
import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.time.Clock;
import java.time.Instant;
import java.time.temporal.TemporalUnit;
import java.util.Arrays;
import java.util.Collections;
import java.util.function.Function;
import org.dcache.util.Result;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;


public class ParsableFileTest {

    private FileSystem fs;
    private AdvanceableClock testClock;

    @Before
    public void setup() {
        fs = Jimfs.newFileSystem(Configuration.unix());
        testClock = new AdvanceableClock(Clock.systemUTC());
    }

    @Test
    public void shouldLoadFile() throws Exception {
        var model = givenAModel();
        var path = given(aFile().withName("omnisession.conf").withContent("DEFAULT home:/ root:/"));
        var parser = given(aParser().thatReturns(model));
        var file = givenParsableFileOf(parser, path);

        Result<Model,String> received = file.get();

        assertThat(received, isSuccessfulAnd(is(theInstance(model))));
        verify(parser).apply(path);
    }

    @Test(expected = NullPointerException.class)
    public void shouldRejectNullModel() throws Exception {
        var path = given(aFile().withName("omnisession.conf").withContent("DEFAULT home:/ root:/"));
        var parser = given(aParser().thatReturnsNull());
        var file = givenParsableFileOf(parser, path);

        file.get();
    }

    @Test
    public void shouldNotLoadFileAgainIfNothingChanged() throws Exception {
        var model = givenAModel();
        var path = given(aFile().withName("omnisession.conf").withContent("DEFAULT home:/ root:/"));
        var parser = given(aParser().thatReturns(model));
        var file = givenParsableFileOf(parser, path);
        givenFileHasBeenRead(file);
        verify(parser).apply(path);

        Result<Model,String> received = file.get();

        assertThat(received, isSuccessfulAnd(is(theInstance(model))));
        verifyNoMoreInteractions(parser);
    }

    @Test
    public void shouldReloadUpdatedFile() throws Exception {
        var model1 = givenAModel();
        var path = given(aFile().withName("omnisession.conf").withContent("DEFAULT home:/ root:/"));
        var parser = given(aParser().thatReturns(model1));
        var file = givenParsableFileOf(parser, path);
        givenFileHasBeenRead(file);
        givenTimeHasAdvacedBy(2, SECONDS);
        givenFileTouched(path);
        var model2 = givenAModel();
        givenParser(parser).nowReturns(model2);

        Result<Model,String> received = file.get();

        verify(parser).apply(path);
        assertThat(received, isSuccessfulAnd(is(theInstance(model2))));
    }

    @Test
    public void shouldNotReloadFileIfNotModified() throws Exception {
        var model = givenAModel();
        var path = given(aFile().withName("omnisession.conf").withContent("DEFAULT home:/ root:/"));
        var parser = given(aParser().thatReturns(model));
        var file = givenParsableFileOf(parser, path);
        givenFileHasBeenRead(file);
        givenTimeHasAdvacedBy(2, SECONDS);

        Result<Model,String> received = file.get();

        verify(parser).apply(path);
        assertThat(received, isSuccessfulAnd(is(theInstance(model))));
    }

    @Test
    public void shouldAttemptReloadIfFileUpdatedAndIsNowBad() throws Exception {
        var model = givenAModel();
        var path = given(aFile().withName("omnisession.conf").withContent("DEFAULT home:/ root:/"));
        var parser = given(aParser().thatReturns(model));
        var file = givenParsableFileOf(parser, path);
        givenFileHasBeenRead(file);
        givenTimeHasAdvacedBy(2, SECONDS);
        givenFileTouched(path);
        givenParser(parser).nowReturnsFailure("Cannot parse");

        Result<Model,String> received = file.get();

        verify(parser).apply(path);
        assertThat(received, isFailureAnd(containsString("Cannot parse")));
    }

    @Test
    public void shouldReloadFileUpdatedFromBadToGood() throws Exception {
        var path = given(aFile().withName("omnisession.conf").withContent("INVALID DATA"));
        var parser = given(aParser().thatReturnsFailure("Cannot parse file."));
        var file = givenParsableFileOf(parser, path);
        givenFileHasBeenRead(file);
        givenTimeHasAdvacedBy(2, SECONDS);
        givenFileTouched(path);
        var model = givenAModel();
        givenParser(parser).nowReturns(model);

        Result<Model,String> received = file.get();

        verify(parser).apply(path);
        assertThat(received, isSuccessfulAnd(is(theInstance(model))));
    }

    @Test
    public void shouldReturnEmptyIfFileDoesNotExist() throws Exception {
        var path = given(aFile().withName("omnisession.conf").thatDoesNotExist());
        var parser = given(aParser().thatReturnsFailure("No such file."));
        var file = givenParsableFileOf(parser, path);

        Result<Model,String> received = file.get();

        verify(parser, never()).apply(any());
    }

    @Test
    public void shouldReturnContentsRereadingNewlyWrittenFile() throws Exception {
        var path = given(aFile().withName("omnisession.conf").thatDoesNotExist());
        var parser = given(aParser().thatReturnsFailure("No such file."));
        var file = givenParsableFileOf(parser, path);
        givenFileHasBeenRead(file);
        givenTimeHasAdvacedBy(2, SECONDS);
        givenFileUpdate(path, "DEFAULT file:/home root:/");
        var model = givenAModel();
        givenParser(parser).nowReturns(model);

        Result<Model,String> received = file.get();

        verify(parser).apply(path);
        assertThat(received, isSuccessfulAnd(is(theInstance(model))));
    }

    private Path given(FileBuilder builder) throws IOException {
        return builder.build();
    }

    private void givenTimeHasAdvacedBy(int amount, TemporalUnit unit) {
        testClock.advance(amount, unit);
    }

    private Function<Path, Result<Model,String>> given(ParserBuilder builder) {
        return builder.build();
    }

    private MockAdjuster givenParser(Function<Path, Result<Model,String>> mock) {
        return new MockAdjuster(mock);
    }

    private void givenFileTouched(Path file) throws IOException, InterruptedException {
        FileTime originalFileTime = Files.getLastModifiedTime(file);

        Files.setLastModifiedTime(file, FileTime.from(Instant.now()));

        // We don't know the granularity of the filesystem's mtime, so let's
        // keep trying until we succeed.
        while (Files.getLastModifiedTime(file).equals(originalFileTime)) {
            System.out.println("Redoing file touch.");
            Thread.sleep(100);
            Files.setLastModifiedTime(file, FileTime.from(Instant.now()));
        }
    }

    private void givenFileUpdate(Path file, String contents) throws IOException {
        Files.writeString(file, contents);
    }

    private Model givenAModel() {
        return mock(Model.class);
    }

    private FileBuilder aFile() {
        return new FileBuilder();
    }

    private ParserBuilder aParser() {
        return new ParserBuilder();
    }

    private ParsableFile givenParsableFileOf(Function<Path, Result<Model,String>> parser, Path path) {
        return new ParsableFile(testClock, parser, path);
    }

    private void givenFileHasBeenRead(ParsableFile file) {
        file.get();
    }

    private class FileBuilder {

        private String name;
        private Path directory = fs.getRootDirectories().iterator().next();
        private StringBuilder contents = new StringBuilder();
        private boolean exists = true;

        public FileBuilder withName(String name) {
            this.name = requireNonNull(name);
            return this;
        }

        public FileBuilder inDirectory(String path) throws IOException {
            directory = fs.getPath(path);
            Files.createDirectories(directory);
            return this;
        }

        public FileBuilder withContent(String... lines) throws IOException {
            Arrays.stream(lines).forEach(l -> contents.append(l).append('\n'));
            return this;
        }

        public FileBuilder thatDoesNotExist() {
            exists = false;
            return this;
        }

        public Path build() throws IOException {
            checkState(name != null, "Missing filename");
            Path file = directory.resolve(name);

            if (exists) {
                if (contents.length() == 0) {
                    Files.createFile(file);
                } else {
                    Files.write(directory.resolve(name), Collections.singleton(contents));
                }
            }

            return file;
        }
    }

    private class ParserBuilder<Model> {

        private final Function<Path, Result<Model,String>> parser = mock(Function.class);

        public ParserBuilder thatReturns(Model model) {
            when(parser.apply(any())).thenReturn(Result.success(model));
            return this;
        }

        public ParserBuilder thatReturnsNull() {
            when(parser.apply(any())).thenReturn(null);
            return this;
        }

        public ParserBuilder thatReturnsFailure(String error) {
            when(parser.apply(any())).thenReturn(Result.failure(error));
            return this;
        }

        public Function<Path, Result<Model,String>> build() {
            return parser;
        }
    }

    /**
     * This class is very ugly -- or, at least, it hides the ugliness of calling the Mockito#reset
     * method.
     */
    private class MockAdjuster {

        private final Function<Path, Result<Model,String>> mock;

        MockAdjuster(Function<Path, Result<Model,String>> mock) {
            this.mock = mock;
        }

        public void nowReturns(Model model) {
            Mockito.reset(mock);
            when(mock.apply(any())).thenReturn(Result.success(model));
        }

        public void nowReturnsFailure(String error) {
            Mockito.reset(mock);
            when(mock.apply(any())).thenReturn(Result.failure(error));
        }
    }

    public static <S,F> Matcher<Result<S,F>> isSuccessfulAnd(Matcher<? super S> matcher) {
        return new TypeSafeMatcher<Result<S,F>>() {
            @Override
            protected boolean matchesSafely(Result<S, F> item) {
                return item.isSuccessful() && matcher.matches(item.getSuccess().get());
            }

            @Override
            public void describeTo(Description description) {
                description.appendText("is successful and ").appendDescriptionOf(matcher);
            }
        };
    }

    public static <S,F> Matcher<Result<S,F>> isFailureAnd(Matcher<? super F> matcher) {
        return new TypeSafeMatcher<Result<S,F>>() {
            @Override
            protected boolean matchesSafely(Result<S,F> item) {
                return item.isFailure() && matcher.matches(item.getFailure().get());
            }

            @Override
            public void describeTo(Description description) {
                description.appendText("is failure and ").appendDescriptionOf(matcher);
            }
        };
    }


    /**
     * The model representing the file's contents.
     */
    private interface Model {

    }
}
