package org.dcache.services.billing.text;

import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import com.google.common.collect.TreeTraverser;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import com.google.common.io.ByteSource;
import com.google.common.io.CharSource;
import com.google.common.io.CharStreams;
import com.google.common.io.Files;
import com.google.common.io.LineProcessor;
import com.google.gson.stream.JsonWriter;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.bridge.SLF4JBridgeHandler;

import java.io.BufferedInputStream;
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PipedReader;
import java.io.PipedWriter;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.Reader;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.MonthDay;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.format.FormatStyle;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.LogManager;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.dcache.boot.LayoutBuilder;
import org.dcache.util.Args;
import org.dcache.util.ConfigurationProperties;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.io.Files.isFile;
import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;

public class Indexer
{
    private static final Logger LOGGER = LoggerFactory.getLogger(Indexer.class);

    private static final Pattern BILLING_NAME_PATTERN =
            Pattern.compile("^billing-(\\d\\d\\d\\d.\\d\\d.\\d\\d)(\\.bz2)?$");
    private static final String BILLING_TEXT_FLAT_DIR = "billing.text.flat-dir";
    private static final String BILLING_TEXT_DIR = "billing.text.dir";
    private static final String BILLING_TEXT_FORMAT_PREFIX = "billing.parser.format!";
    private static final String BZ2 = "bz2";
    private static final int PIPE_SIZE = 2048;

    private static final DateTimeFormatter CLI_DATE_FORMAT =
            DateTimeFormatter.ofLocalizedDate(FormatStyle.MEDIUM);
    private static final DateTimeFormatter DEFAULT_DATE_FORMAT =
            DateTimeFormatter.ofPattern("MM.dd");
    private static final DateTimeFormatter DEFAULT_TIME_FORMAT =
            DateTimeFormatter.ofPattern("HH:mm:ss");
    private static final DateTimeFormatter FILE_DATE_FORMAT =
            DateTimeFormatter.ofPattern("uuuu.MM.dd");
    private static final DateTimeFormatter DIRECTORY_DATE_FORMAT =
            DateTimeFormatter.ofPattern("uuuu" + File.separator + "MM");
    private static final DateTimeFormatter ISO8601_FORMAT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ssX");

    /**
     * Almost identical to the file tree traverser from Guava, sorts directory entries
     * lexicographically.
     */
    private static final TreeTraverser<File> SORTED_FILE_TREE_TRAVERSER = new TreeTraverser<File>() {
        @Override
        public Iterable<File> children(File file) {
            // check isDirectory() just because it may be faster than listFiles() on a non-directory
            if (file.isDirectory()) {
                File[] files = file.listFiles();
                if (files != null) {
                    return Ordering.natural().sortedCopy(asList(files));
                }
            }

            return Collections.emptyList();
        }
    };

    private final boolean isFlat;
    private final File dir;
    private final ImmutableMap<String, String> formats;

    private Indexer(Args args) throws IOException, URISyntaxException, ClassNotFoundException
    {
        double fpp = args.getDoubleOption("fpp", 0.01);

        ConfigurationProperties configuration = new LayoutBuilder().build().properties();
        isFlat = Boolean.valueOf(args.getOption("flat", configuration.getValue(BILLING_TEXT_FLAT_DIR)));
        dir = new File(args.getOption("dir", configuration.getValue(BILLING_TEXT_DIR)));
        formats = getBillingFormats(configuration);

        if (args.hasOption("find")) {
            Collection<String> searchTerms;
            if (args.hasOption("f")) {
                searchTerms = Files.readLines(new File(args.getOption("f")), Charsets.UTF_8);
            } else if (args.argc() > 0) {
                searchTerms = args.getArguments();
            } else {
                searchTerms = ImmutableList.of("");
            }

            FluentIterable<File> filesWithPossibleMatch =
                    SORTED_FILE_TREE_TRAVERSER
                            .preOrderTraversal(dir);
            if (args.hasOption("since") || args.hasOption("until")) {
                LocalDate since = args.hasOption("since")
                        ? LocalDate.parse(args.getOption("since"), CLI_DATE_FORMAT) : LocalDate.ofEpochDay(0);
                LocalDate until = args.hasOption("until")
                        ? LocalDate.parse(args.getOption("until"), CLI_DATE_FORMAT) : LocalDate.now().plusDays(1);
                filesWithPossibleMatch =
                        filesWithPossibleMatch.filter(file -> isInRange(file, since, until));
            }
            if (searchTerms.contains("")) {
                filesWithPossibleMatch =
                        filesWithPossibleMatch.filter(file -> isBillingFile(file));
            } else {
                filesWithPossibleMatch =
                        filesWithPossibleMatch.filter(isBillingFileAndMightContain(searchTerms));
            }

            if (args.hasOption("files")) {
                for (File file : filesWithPossibleMatch) {
                    System.out.println(file);
                }
            } else if (args.hasOption("yaml")) {
                try (OutputWriter out = toYaml(System.out)) {
                    find(searchTerms, filesWithPossibleMatch, out);
                }
            } else if (args.hasOption("json")) {
                try (OutputWriter out = toJson(System.out)) {
                    find(searchTerms, filesWithPossibleMatch, out);
                }
            } else {
                try (OutputWriter out = toText(System.out)) {
                    find(searchTerms, filesWithPossibleMatch, out);
                }
            }
        } else if (args.hasOption("all")) {
            for (File file : SORTED_FILE_TREE_TRAVERSER.preOrderTraversal(dir).filter(isFile())) {
                Matcher matcher = BILLING_NAME_PATTERN.matcher(file.getName());
                if (matcher.matches()) {
                    System.out.println("Indexing " + file);
                    index(fpp, file, getIndexFile(file.getParentFile(), matcher.group(1)));
                }
            }
        } else if (args.hasOption("yesterday")) {
            LocalDate yesterday = LocalDate.now().minusDays(1);
            File billingFile = getBillingFile(yesterday);
            File errorFile = getErrorFile(yesterday);
            File indexFile = getIndexFile(yesterday);
            if (billingFile.exists()) {
                index(fpp, billingFile, indexFile);
                if (args.hasOption("compress")) {
                    compress(billingFile);
                }
            }
            if (errorFile.exists() && args.hasOption("compress")) {
                compress(errorFile);
            }
        } else if (args.hasOption("index")) {
            for (String name : args.getArguments()) {
                File file = new File(name);
                Matcher matcher = BILLING_NAME_PATTERN.matcher(file.getName());
                if (!matcher.matches()) {
                    throw new IllegalArgumentException("File name does not follow the format of billing files: " + name);
                }
                index(fpp, file, getIndexFile(file.getParentFile(), matcher.group(1)));
            }
        } else if (args.hasOption("compress")) {
            for (String name : args.getArguments()) {
                compress(new File(name));
            }
        } else if (args.hasOption("decompress")) {
            for (String name : args.getArguments()) {
                decompress(new File(name));
            }
        } else if (args.hasOption("help")) {
            help(System.err);
        } else {
            throw new IllegalArgumentException("Invalid arguments.");
        }
    }

    private OutputWriter toText(final PrintStream out)
    {
        return new OutputWriter()
        {
            @Override
            public void write(LocalDate date, String line) throws IOException
            {
                if (!line.isEmpty() && line.charAt(0) != '#') {
                    // Prepend year if the default timestamp format is used
                    try {
                        int year = date.getYear();
                        parseDefaultTimestamp(year, line);
                        out.append(String.valueOf(year)).append('.');
                    } catch (DateTimeParseException ignore) {
                    }
                    out.println(line);
                }
            }

            @Override
            public void close()
            {
            }
        };
    }

    private OutputWriter toJson(final PrintStream out) throws IOException, URISyntaxException
    {
        return new OutputWriter()
        {
            final JsonWriter writer = new JsonWriter(new OutputStreamWriter(out));

            final BillingParserBuilder builder =
                    new BillingParserBuilder(formats).addAllAttributes();

            Function<String, Map<String, String>> parser = builder.buildToMap();

            {
                writer.setIndent("  ");
                writer.beginArray();
            }

            @Override
            public void write(LocalDate date, String line) throws IOException
            {
                if (line.startsWith("##")) {
                    parser = builder.withFormat(line).addAllAttributes().buildToMap();
                } else {
                    Map<String, String> attributes = parser.apply(line);
                    if (!attributes.isEmpty()) {
                        fixDate(date.getYear(), attributes);
                        writer.beginObject();
                        for (Map.Entry<String, String> entry : attributes.entrySet()) {
                            writer.name(entry.getKey()).value(entry.getValue());
                        }
                        writer.endObject();
                    }
                }
            }

            @Override
            public void close() throws IOException
            {
                writer.endArray();
                writer.flush();
                out.println();
            }
        };
    }

    private OutputWriter toYaml(final PrintStream out) throws IOException, URISyntaxException
    {
        return new OutputWriter()
        {
            final BillingParserBuilder builder =
                    new BillingParserBuilder(formats).addAllAttributes();

            Function<String, Map<String, String>> parser = builder.buildToMap();

            @Override
            public void write(LocalDate date, String line) throws IOException
            {
                if (line.startsWith("##")) {
                    parser = builder.withFormat(line).addAllAttributes().buildToMap();
                } else {
                    Map<String, String> attributes = parser.apply(line);
                    if (attributes.isEmpty()) {
                        out.append("# Unknown: ").println(line);
                    } else {
                        fixDate(date.getYear(), attributes);
                        out.append("# ").println(line);
                        String format = "- %-21s %s\n";
                        for (Map.Entry<String, String> entry : attributes.entrySet()) {
                            out.printf(format, entry.getKey() + ':', entry.getValue());
                            format = "  %-21s %s\n";
                        }
                    }
                }
            }

            @Override
            public void close()
            {
            }
        };
    }

    /**
     * Searches for searchTerm in files and writes any matching lines to out.
     */
    private static void find(final Collection<String> searchTerms, FluentIterable<File> files, final OutputWriter out)
            throws IOException
    {
        int threads = Runtime.getRuntime().availableProcessors();
        ExecutorService executor = Executors.newFixedThreadPool(threads);
        try {
            List<Map.Entry<LocalDate,Reader>> readers = new ArrayList<>();
            for (File file : files) {
                Matcher matcher = BILLING_NAME_PATTERN.matcher(file.getName());
                if (matcher.matches()) {
                    LocalDate date = LocalDate.parse(matcher.group(1), FILE_DATE_FORMAT);
                    PipedReader reader = new PipedReader(PIPE_SIZE);
                    PipedWriter writer = new PipedWriter(reader);
                    executor.submit(() -> {
                        try {
                            grep(searchTerms, file, new PrintWriter(writer));
                        } finally {
                            writer.close();
                        }
                        return null;
                    });
                    readers.add(Maps.immutableEntry(date, reader));
                }
            }
            for (final Map.Entry<LocalDate, Reader> entry : readers) {
                CharStreams.readLines(entry.getValue(), new LineProcessor<Void>()
                {
                    @Override
                    public boolean processLine(String line) throws IOException
                    {
                        out.write(entry.getKey(), line);
                        return true;
                    }

                    @Override
                    public Void getResult()
                    {
                        return null;
                    }
                });
            }
        } finally {
            executor.shutdown();
        }
    }

    private static void grep(final Collection<String> searchTerms, File file, PrintWriter out)
            throws IOException
    {
        asCharSource(file, Charsets.UTF_8).readLines(new LineProcessor<Void>()
        {
            @Override
            public boolean processLine(String line) throws IOException
            {
                if (!line.isEmpty() && line.charAt(0) != '#') {
                    for (String term : searchTerms) {
                        if (line.contains(term)) {
                            out.println(line);
                            break;
                        }
                    }
                } else if (line.startsWith("##")) {
                    out.println(line);
                }
                return true;
            }

            @Override
            public Void getResult()
            {
                return null;
            }
        });
   }

    private void index(double fpp, File billingFile, File indexFile) throws IOException
    {
        int threads = Runtime.getRuntime().availableProcessors();
        Set<String> index = produceIndex(billingFile, threads);
        BloomFilter<CharSequence> filter = produceBloomFilter(fpp, index);
        writeToFile(indexFile, filter);
    }

    private static void decompress(File compressedFile) throws IOException
    {
        String path = compressedFile.getPath();
        checkArgument(Files.getFileExtension(path).equals(BZ2), "File must have " + BZ2 + " extension.");
        File file = new File(compressedFile.getParent(), Files.getNameWithoutExtension(path));
        try (InputStream in = new BZip2CompressorInputStream(new BufferedInputStream(new FileInputStream(file)))) {
            Files.asByteSink(file).writeFrom(in);
        }
        java.nio.file.Files.delete(compressedFile.toPath());
    }

    private static void compress(File file) throws IOException
    {
        File compressedFile = new File(file.getPath() + "." + BZ2);
        try (OutputStream out = new BZip2CompressorOutputStream(Files.asByteSink(compressedFile).openBufferedStream())) {
            Files.asByteSource(file).copyTo(out);
        }
        java.nio.file.Files.delete(file.toPath());
    }

    private static void help(PrintStream out)
    {
        out.println("COMMANDS:");
        out.println("   -all [-fpp=PROP] [-dir=BASE]");
        out.println("          (Re)index all billing files.");
        out.println("   -compress FILE...");
        out.println("          Compress FILE.");
        out.println("   -decompress FILE...");
        out.println("          Decompress FILE.");
        out.println("   -find [-files|-json|-yaml] [-dir=BASE] [-since=DATE] [-until=DATE] [-f=FILE] [SEARCHTERM]...");
        out.println("          Output billing entries that contain SEARCHTERM. Valid search terms are");
        out.println("          path, pnfsid, dn and path prefixes of those. Optionally output names");
        out.println("          of billing files that might contain the search term. If no search term");
        out.println("          is provided, all entries are output.");
        out.println("   -index [-fpp=PROP] FILE...");
        out.println("          Create index for FILE.");
        out.println("   -yesterday [-compress] [-fpp=PROP] [-dir=BASE] [-flat=BOOL]");
        out.println("          Index yesterday's billing file. Optionally compresses the billing file");
        out.println("          after indexing it.");
        out.println("");
        out.println("OPTIONS:");
        out.println("   -dir=BASE");
        out.println("          Base directory for billing files. Default is taken from dCache");
        out.println("          configuration.");
        out.println("   -flat=BOOLEAN");
        out.println("          Chooses between flat or hierarchical directory layout. Default is");
        out.println("          taken from dCache configuration.");
        out.println("   -fpp=PROP");
        out.println("          The false positive probability expressed as a value in (0;1]. The");
        out.println("          default is 0.01.");
    }

    private static LocalDateTime parseDefaultTimestamp(int year, String s)
            throws DateTimeParseException
    {
        MonthDay monthDay = MonthDay.parse(s.substring(0, 5), DEFAULT_DATE_FORMAT);
        LocalTime time = LocalTime.parse(s.substring(6, 14), DEFAULT_TIME_FORMAT);
        return monthDay.atYear(year).atTime(time);
    }

    /**
     * Completes the date field of billing entries by adding a year to it.
     */
    private static void fixDate(int year, Map<String,String> attributes)
    {
        String s = attributes.get("date");
        if (s != null) {
            try {
                LocalDateTime timestamp = parseDefaultTimestamp(year, s);
                attributes.put("date", ISO8601_FORMAT.format(timestamp.atZone(ZoneId.systemDefault())));
            } catch (DateTimeParseException ignore) {
            }
        }
    }

    private File getDirectory(LocalDate date)
    {
        return isFlat ? dir : new File(this.dir, DIRECTORY_DATE_FORMAT.format(date));
    }

    private File getBillingFile(LocalDate date)
    {
        return new File(getDirectory(date), "billing-" + FILE_DATE_FORMAT.format(date));
    }

    private File getErrorFile(LocalDate date)
    {
        return new File(getDirectory(date), "billing-error-" + FILE_DATE_FORMAT.format(date));
    }

    private File getIndexFile(LocalDate date)
    {
        return getIndexFile(getDirectory(date), FILE_DATE_FORMAT.format(date));
    }

    private static File getIndexFile(File dir, String date)
    {
        return new File(dir, "index-" + date);
    }

    private Set<String> produceIndex(final File file, int threads)
            throws IOException
    {
        try {
            IndexProcessor processor = new IndexProcessor(formats);
            Set<String> index;
            try (ParallelizingLineProcessor<Set<String>> parallelizer = new ParallelizingLineProcessor<>(threads, processor)) {
                index = asCharSource(file, Charsets.UTF_8).readLines(parallelizer);
            }
            return index;
        } catch (IOException e) {
            throw new IOException("I/O failure while reading " + file + ":" + e.getMessage(), e);
        } catch (URISyntaxException e) {
            throw new IOException("Invalid dCache configuration: " + e.getMessage(), e);
        }
    }

    private static CharSource asCharSource(final File file, Charset charset)
    {
        ByteSource source;
        if (Files.getFileExtension(file.getPath()).equals(BZ2)) {
            source = new ByteSource() {
                @Override
                public InputStream openStream() throws IOException
                {
                    return new BZip2CompressorInputStream(new BufferedInputStream(new FileInputStream(file)));
                }
            };
        } else {
            source = Files.asByteSource(file);
        }
        return source.asCharSource(charset);
    }

    private static BloomFilter<CharSequence> produceBloomFilter(double fpp, Set<String> index)
    {
        BloomFilter<CharSequence> filter =
                BloomFilter.create(Funnels.stringFunnel(Charsets.UTF_8), index.size(), fpp);
        index.forEach(filter::put);
        return filter;
    }

    private static void writeToFile(File outFile, Object object)
            throws IOException
    {
        try (ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(outFile))) {
            out.writeObject(object);
        }
    }

    private static Object readFromFile(File outFile)
            throws IOException, ClassNotFoundException
    {
        try (ObjectInputStream in = new ObjectInputStream(new FileInputStream(outFile))) {
            return in.readObject();
        }
    }

    private static boolean isBillingFile(File file)
    {
        if (!file.isFile()) {
            return false;
        }
        Matcher matcher = BILLING_NAME_PATTERN.matcher(file.getName());
        return matcher.matches();
    }

    /**
     * Returns all billing format strings from configuration.
     */
    private static ImmutableMap<String,String> getBillingFormats(ConfigurationProperties configuration)
    {
        ImmutableMap.Builder<String,String> formats = ImmutableMap.builder();
        for (String name : configuration.stringPropertyNames()) {
            if (name.startsWith(BILLING_TEXT_FORMAT_PREFIX)) {
                formats.put(name.substring(BILLING_TEXT_FORMAT_PREFIX.length()), configuration.getValue(name));
            }
        }
        return formats.build();
    }

    private static Predicate<File> isBillingFileAndMightContain(Collection<String> terms)
    {
        final List<String> searchTerms = terms.stream()
                .map(str -> str.endsWith("/") ? str.substring(0, str.length() - 1) : str)
                .collect(toList());
        return new Predicate<File>()
        {
            @Override
            public boolean apply(File file)
            {
                if (!file.isFile()) {
                    return false;
                }
                try {
                    Matcher matcher = BILLING_NAME_PATTERN.matcher(file.getName());
                    return matcher.matches() && mightContain(getIndexFile(file.getParentFile(), matcher.group(1)));
                } catch (ClassNotFoundException | IOException e) {
                    throw new RuntimeException("Failed to read index", e);
                }
            }

            @SuppressWarnings("unchecked")
            private boolean mightContain(File index)
                    throws IOException, ClassNotFoundException
            {
                if (!index.exists()) {
                    return true;
                }
                BloomFilter<CharSequence> filter = (BloomFilter<CharSequence>) readFromFile(index);
                for (String term : searchTerms) {
                    if (filter.mightContain(term)) {
                        return true;
                    }
                }
                return false;
            }
        };
    }

    private static boolean isInRange(File file, LocalDate since, LocalDate until)
    {
        Matcher matcher = BILLING_NAME_PATTERN.matcher(file.getName());
        if (matcher.matches()) {
            LocalDate date = LocalDate.parse(matcher.group(1), FILE_DATE_FORMAT);
            if ((date.isEqual(since) || date.isAfter(since)) && date.isBefore(until)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Billing file line processor that collects strings to index.
     */
    private static class IndexProcessor implements LineProcessor<Set<String>>
    {
        private final Set<String> result = Sets.newConcurrentHashSet();
        private final BillingParserBuilder builder;

        private Function<String, String[]> parser;

        private IndexProcessor(ImmutableMap<String, String> formats)
                throws IOException, URISyntaxException
        {
            builder = new BillingParserBuilder(formats)
                    .addAttribute("path")
                    .addAttribute("pnfsid")
                    .addAttribute("owner");
            parser = builder.buildToArray();
        }

        @Override
        public boolean processLine(String line) throws IOException
        {
            if (!line.isEmpty() && line.charAt(0) != '#') {
                String[] value = parser.apply(line);
                if (value[0] != null) {
                    addAllPathPrefixes(value[0], result);
                }
                if (value[1] != null) {
                    result.add(value[1]);
                }
                if (value[2] != null) {
                    addAllPathPrefixes(value[2], result);
                }
            } else if (line.startsWith("##")) {
                parser = builder.withFormat(line).buildToArray();
            }
            return true;
        }

        @Override
        public Set<String> getResult()
        {
            return result;
        }

        private static void addAllPathPrefixes(String path, Set<String> paths)
        {
            int index = 1;
            int next;
            while ((next = path.indexOf('/', index)) != -1) {
                paths.add(path.substring(0, next));
                index = next + 1;
            }
            paths.add(path);
        }
    }

    public static void main(String[] arguments)
            throws URISyntaxException, ExecutionException, InterruptedException,
                   ClassNotFoundException
    {
        LogManager.getLogManager().reset();
        SLF4JBridgeHandler.install();
        Thread.setDefaultUncaughtExceptionHandler((t, e) -> LOGGER.error("Uncaught exception", e));

        try {
            new Indexer(new Args(arguments));
        } catch (IllegalArgumentException e) {
            System.err.println(e.getMessage());
            System.err.println();
            help(System.err);
            System.exit(1);
        } catch (DateTimeParseException e) {
            System.err.println(e.getMessage());
            System.err.println();
            System.exit(1);
        } catch (IOException | URISyntaxException | ClassNotFoundException e) {
            System.err.println(e);
            System.exit(2);
        }
    }

    private interface OutputWriter extends Closeable
    {
        void write(LocalDate date, String line) throws IOException;
    }
}
