/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2021 Deutsches Elektronen-Synchrotron
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
package org.dcache.gplazma.omnisession;

import org.junit.Test;

import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;
import org.junit.Before;
import org.mockito.Mockito;

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
import java.util.Optional;
import java.util.function.Function;

import static com.github.npathai.hamcrestopt.OptionalMatchers.isEmpty;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static com.github.npathai.hamcrestopt.OptionalMatchers.isPresentAnd;
import static java.time.temporal.ChronoUnit.SECONDS;
import static org.hamcrest.CoreMatchers.*;
import static org.mockito.Mockito.*;

public class ParsableFileTest
{
    private FileSystem fs;
    private AdvanceableClock testClock;

    @Before
    public void setup()
    {
        fs = Jimfs.newFileSystem(Configuration.unix());
        testClock = new AdvanceableClock(Clock.systemUTC());
    }

    @Test
    public void shouldLoadFile() throws Exception
    {
        var model = givenAModel();
        var path = given(aFile().withName("omnisession.conf").withContent("DEFAULT home:/ root:/"));
        var parser = given(aParser().thatReturns(model));
        var file = givenParsableFileOf(parser, path);

        Optional<Model> received = file.get();

        assertThat(received, isPresentAnd(is(theInstance(model))));
        verify(parser).apply(path);
    }

    @Test(expected=NullPointerException.class)
    public void shouldRejectNullModel() throws Exception
    {
        var path = given(aFile().withName("omnisession.conf").withContent("DEFAULT home:/ root:/"));
        var parser = given(aParser().thatReturnsNull());
        var file = givenParsableFileOf(parser, path);

        file.get();
    }

    @Test
    public void shouldNotLoadFileAgainIfNothingChanged() throws Exception
    {
        var model = givenAModel();
        var path = given(aFile().withName("omnisession.conf").withContent("DEFAULT home:/ root:/"));
        var parser = given(aParser().thatReturns(model));
        var file = givenParsableFileOf(parser, path);
        givenFileHasBeenRead(file);
        verify(parser).apply(path);

        Optional<Model> received = file.get();

        assertThat(received, isPresentAnd(is(theInstance(model))));
        verifyNoMoreInteractions(parser);
    }

    @Test
    public void shouldReloadUpdatedFile() throws Exception
    {
        var model1 = givenAModel();
        var path = given(aFile().withName("omnisession.conf").withContent("DEFAULT home:/ root:/"));
        var parser = given(aParser().thatReturns(model1));
        var file = givenParsableFileOf(parser, path);
        givenFileHasBeenRead(file);
        givenTimeHasAdvacedBy(2, SECONDS);
        givenFileTouched(path);
        var model2 = givenAModel();
        givenParser(parser).nowReturns(model2);

        Optional<Model> received = file.get();

        verify(parser).apply(path);
        assertThat(received, isPresentAnd(is(theInstance(model2))));
    }

    @Test
    public void shouldNotReloadFileIfNotModified() throws Exception
    {
        var model = givenAModel();
        var path = given(aFile().withName("omnisession.conf").withContent("DEFAULT home:/ root:/"));
        var parser = given(aParser().thatReturns(model));
        var file = givenParsableFileOf(parser, path);
        givenFileHasBeenRead(file);
        givenTimeHasAdvacedBy(2, SECONDS);

        Optional<Model> received = file.get();

        verify(parser).apply(path);
        assertThat(received, isPresentAnd(is(theInstance(model))));
    }

    @Test
    public void shouldAttemptReloadIfFileUpdatedAndIsNowBad() throws Exception
    {
        var model = givenAModel();
        var path = given(aFile().withName("omnisession.conf").withContent("DEFAULT home:/ root:/"));
        var parser = given(aParser().thatReturns(model));
        var file = givenParsableFileOf(parser, path);
        givenFileHasBeenRead(file);
        givenTimeHasAdvacedBy(2, SECONDS);
        givenFileTouched(path);
        givenParser(parser).nowReturnsEmpty();

        Optional<Model> received = file.get();

        verify(parser).apply(path);
        assertThat(received, isEmpty());
    }

    @Test
    public void shouldReloadFileUpdatedFromBadToGood() throws Exception
    {
        var path = given(aFile().withName("omnisession.conf").withContent("INVALID DATA"));
        var parser = given(aParser().thatReturnsEmpty());
        var file = givenParsableFileOf(parser, path);
        givenFileHasBeenRead(file);
        givenTimeHasAdvacedBy(2, SECONDS);
        givenFileTouched(path);
        var model = givenAModel();
        givenParser(parser).nowReturns(model);

        Optional<Model> received = file.get();

        verify(parser).apply(path);
        assertThat(received, isPresentAnd(is(theInstance(model))));
    }

    @Test
    public void shouldReturnEmptyIfFileDoesNotExist() throws Exception
    {
        var path = given(aFile().withName("omnisession.conf").thatDoesNotExist());
        var parser = given(aParser().thatReturnsEmpty());
        var file = givenParsableFileOf(parser, path);

        Optional<Model> received = file.get();

        verify(parser,never()).apply(any());
    }

    @Test
    public void shouldReturnContentsRereadingNewlyWrittenFile() throws Exception
    {
        var path = given(aFile().withName("omnisession.conf").thatDoesNotExist());
        var parser = given(aParser().thatReturnsEmpty());
        var file = givenParsableFileOf(parser, path);
        givenFileHasBeenRead(file);
        givenTimeHasAdvacedBy(2, SECONDS);
        givenFileUpdate(path, "DEFAULT file:/home root:/");
        var model = givenAModel();
        givenParser(parser).nowReturns(model);

        Optional<Model> received = file.get();

        verify(parser).apply(path);
        assertThat(received, isPresentAnd(is(theInstance(model))));
    }

    private Path given(FileBuilder builder) throws IOException
    {
        return builder.build();
    }

    private void givenTimeHasAdvacedBy(int amount, TemporalUnit unit)
    {
        testClock.advance(amount, unit);
    }

    private Function<Path,Optional<Model>> given(ParserBuilder builder)
    {
        return builder.build();
    }

    private MockAdjuster givenParser(Function<Path,Optional<Model>> mock)
    {
        return new MockAdjuster(mock);
    }

    private void givenFileTouched(Path file) throws IOException, InterruptedException
    {
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

    private void givenFileUpdate(Path file, String contents) throws IOException
    {
        Files.writeString(file, contents);
    }

    private Model givenAModel()
    {
        return mock(Model.class);
    }

    private FileBuilder aFile()
    {
        return new FileBuilder();
    }

    private ParserBuilder aParser()
    {
        return new ParserBuilder();
    }

    private ParsableFile givenParsableFileOf(Function<Path,Optional<Model>> parser, Path path)
    {
        return new ParsableFile(testClock, parser, path);
    }

    private void givenFileHasBeenRead(ParsableFile file)
    {
        file.get();
    }

    private class FileBuilder
    {
        private String name;
        private Path directory = fs.getRootDirectories().iterator().next();
        private StringBuilder contents = new StringBuilder();
        private boolean exists = true;

        public FileBuilder withName(String name)
        {
            this.name = requireNonNull(name);
            return this;
        }

        public FileBuilder inDirectory(String path) throws IOException
        {
            directory = fs.getPath(path);
            Files.createDirectories(directory);
            return this;
        }

        public FileBuilder withContent(String... lines) throws IOException
        {
            Arrays.stream(lines).forEach(l -> contents.append(l).append('\n'));
            return this;
        }

        public FileBuilder thatDoesNotExist()
        {
            exists = false;
            return this;
        }

        public Path build() throws IOException
        {
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

    private class ParserBuilder<Model>
    {
        private final Function<Path,Optional<Model>> parser = mock(Function.class);

        public ParserBuilder thatReturns(Model model)
        {
            when(parser.apply(any())).thenReturn(Optional.of(model));
            return this;
        }

        public ParserBuilder thatReturnsNull()
        {
            when(parser.apply(any())).thenReturn(null);
            return this;
        }

        public ParserBuilder thatReturnsEmpty()
        {
            when(parser.apply(any())).thenReturn(Optional.empty());
            return this;
        }

        public Function<Path,Optional<Model>> build()
        {
            return parser;
        }
    }

    /**
     * This class is very ugly -- or, at least, it hides the ugliness of
     * calling the Mockito#reset method.
     */
    private class MockAdjuster
    {
        private final Function<Path,Optional<Model>> mock;

        MockAdjuster(Function<Path,Optional<Model>> mock)
        {
            this.mock = mock;
        }

        public void nowReturns(Model model)
        {
            Mockito.reset(mock);
            when(mock.apply(any())).thenReturn(Optional.of(model));
        }

        public void nowReturnsEmpty()
        {
            Mockito.reset(mock);
            when(mock.apply(any())).thenReturn(Optional.empty());
        }
    }

    /**
     * The model representing the file's contents.
     */
    private interface Model
    {
    }
}
