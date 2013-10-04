package org.dcache.services.billing.text;

import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Sets;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import com.google.common.io.CharStreams;
import com.google.common.io.Files;
import com.google.common.io.InputSupplier;
import com.google.common.io.LineProcessor;
import com.google.common.io.OutputSupplier;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.bridge.SLF4JBridgeHandler;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.logging.LogManager;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import dmg.util.Args;

import org.dcache.boot.LayoutBuilder;
import org.dcache.util.ConfigurationProperties;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.io.Files.fileTreeTraverser;
import static com.google.common.io.Files.isFile;

public class Indexer
{
    private static final Logger LOGGER = LoggerFactory.getLogger(Indexer.class);

    private static final Pattern BILLING_NAME_PATTERN = Pattern.compile("^billing-(\\d\\d\\d\\d.\\d\\d.\\d\\d)(\\.bz2)?$");
    private static final String BILLING_TEXT_FLAT_DIR = "billing.text.flat-dir";
    private static final String BILLING_TEXT_DIR = "billing.text.dir";
    private static final String BZ_SUFFIX = ".bz2";

    private final ConfigurationProperties configuration;
    private final boolean isFlat;
    private final File dir;

    private final SimpleDateFormat fileNameFormat =
            new SimpleDateFormat("yyyy.MM.dd");
    private final SimpleDateFormat directoryNameFormat =
            new SimpleDateFormat("yyyy" + File.separator + "MM");

    private Indexer(Args args) throws IOException, URISyntaxException, ClassNotFoundException
    {
        double fpp = args.getDoubleOption("fpp", 0.01);

        configuration = new LayoutBuilder().build().properties();
        isFlat = Boolean.valueOf(args.getOption("flat", configuration.getValue(BILLING_TEXT_FLAT_DIR)));
        dir = new File(args.getOption("dir", configuration.getValue(BILLING_TEXT_DIR)));

        if (args.hasOption("find")) {
            boolean shouldOutputFilesNames = args.hasOption("files");
            String searchTerm = args.argv(0);
            FluentIterable<File> filesWithPossibleMatch =
                    fileTreeTraverser().preOrderTraversal(dir).filter(isBillingFileAndMightContain(searchTerm));
            if (shouldOutputFilesNames) {
                for (File file : filesWithPossibleMatch) {
                    System.out.println(file);
                }
            } else {
                for (File file : filesWithPossibleMatch) {
                    grep(searchTerm, file);
                }
            }
        } else if (args.hasOption("all")) {
            for (File file : fileTreeTraverser().preOrderTraversal(dir).filter(isFile())) {
                Matcher matcher = BILLING_NAME_PATTERN.matcher(file.getName());
                if (matcher.matches()) {
                    System.out.println("Indexing " + file);
                    index(fpp, file, new File(file.getParentFile(), matcher.replaceAll("index-$1")));
                }
            }
        } else if (args.hasOption("yesterday")) {
            Date yesterday = getYesterday();
            File billingFile = getBillingFile(yesterday);
            File indexFile = getIndexFile(yesterday);
            if (billingFile.exists()) {
                index(fpp, billingFile, indexFile);
                if (args.hasOption("compress")) {
                    compress(billingFile);
                }
            }
        } else if (args.hasOption("index")) {
            File file = new File(args.argv(0));
            Matcher matcher = BILLING_NAME_PATTERN.matcher(file.getName());
            if (!matcher.matches()) {
                System.err.println("File name not follow the format of billing files");
                System.exit(1);
            }
            index(fpp, file, new File(file.getParentFile(), matcher.replaceAll("index-$1")));
        } else if (args.hasOption("compress")) {
            compress(new File(args.argv(0)));
        } else if (args.hasOption("decompress")) {
            decompress(new File(args.argv(0)));
        } else if (args.hasOption("help")) {
            help();
        } else {
            System.err.println("Invalid arguments");
            System.exit(1);
        }
    }

    private void grep(final String searchTerm, File file) throws IOException
    {
        CharStreams.readLines(newReaderSupplier(file, Charsets.UTF_8), new LineProcessor<Void>()
        {
            @Override
            public boolean processLine(String line) throws IOException
            {
                if (line.contains(searchTerm)) {
                    System.out.println(line);
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

    private void decompress(File compressedFile) throws IOException
    {
        String path = compressedFile.getPath();
        checkArgument(path.endsWith(BZ_SUFFIX), "File must have " + BZ_SUFFIX + " ending");
        File billingFile = new File(path.substring(0, path.length() - BZ_SUFFIX.length()));
        Files.copy(new Bzip2CompressorInputStreamSupplier(compressedFile), billingFile);
        compressedFile.delete();
    }

    private void compress(File billingFile) throws IOException
    {
        File compressedFile = new File(billingFile.getPath() + BZ_SUFFIX);
        Files.copy(billingFile, new Bzip2CompressorOutputStreamSupplier(compressedFile));
        billingFile.delete();
    }

    private static void help()
    {
        System.out.println("COMMANDS:");
        System.out.println("   -all [-fpp=PROP] [-dir=BASE]");
        System.out.println("          (Re)index all billing files.");
        System.out.println("   -compress FILE");
        System.out.println("          Compress FILE.");
        System.out.println("   -decompress FILE");
        System.out.println("          Decompress FILE.");
        System.out.println("   -find [-files] [-dir=BASE] SEARCHTERM");
        System.out.println("          Output billing entries that contain SEARCHTERM. Valid search terms are");
        System.out.println("          path, pnfsid, dn and path prefixes of those. Optionally output names");
        System.out.println("          of billing files that might contain the search term.");
        System.out.println("   -index [-fpp=PROP] FILE");
        System.out.println("          Create index for FILE.");
        System.out.println("   -yesterday [-compress] [-fpp=PROP] [-dir=BASE] [-flat=BOOL]");
        System.out.println("          Index yesterday's billing file. Optionally compresses the billing file");
        System.out.println("          after indexing it.");
        System.out.println("");
        System.out.println("OPTIONS:");
        System.out.println("   -dir=BASE");
        System.out.println("          Base directory for billing files. Default is taken from dCache");
        System.out.println("          configuration.");
        System.out.println("   -flat=BOOLEAN");
        System.out.println("          Chooses between flat or hierarchical directory layout. Default is");
        System.out.println("          taken from dCache configuration.");
        System.out.println("   -fpp=PROP");
        System.out.println("          The false positive probability expressed as a value in (0;1]. The");
        System.out.println("          default is 0.01.");
        System.exit(0);
    }

    private Date getYesterday()
    {
        Calendar cal = Calendar.getInstance();
        cal.add(Calendar.DATE, -1);
        return cal.getTime();
    }

    private synchronized File getDirectory(Date date)
    {
        return isFlat ? dir : new File(this.dir, directoryNameFormat.format(date));
    }

    private synchronized File getBillingFile(Date date)
    {
        return new File(getDirectory(date), "billing-" + fileNameFormat.format(date));
    }

    private synchronized File getIndexFile(Date date)
    {
        return new File(getDirectory(date), "index-" + fileNameFormat.format(date));
    }

    private Set<String> produceIndex(final File file, int threads)
            throws IOException
    {
        try {
            IndexProcessor processor = new IndexProcessor(configuration);
            Set<String> index;
            try (ParallelizingLineProcessor<Set<String>> parallelizer = new ParallelizingLineProcessor<>(threads, processor)) {
                index = CharStreams.readLines(newReaderSupplier(file, Charsets.UTF_8), parallelizer);
            }
            return index;
        } catch (IOException e) {
            throw new IOException("I/O failure while reading " + file + ":" + e.getMessage(), e);
        } catch (URISyntaxException e) {
            throw new IOException("Invalid dCache configuration: " + e.getMessage(), e);
        }
    }

    private static InputSupplier<InputStreamReader> newReaderSupplier(File file, Charset charset)
    {
        InputSupplier<InputStreamReader> input;
        if (file.getName().endsWith(".bz2")) {
            input = CharStreams
                    .newReaderSupplier(new Bzip2CompressorInputStreamSupplier(file), charset);
        } else {
            input = Files.newReaderSupplier(file, charset);
        }
        return input;
    }

    private static BloomFilter<CharSequence> produceBloomFilter(double fpp, Set<String> index)
    {
        BloomFilter<CharSequence> filter =
                BloomFilter.create(Funnels.stringFunnel(Charsets.UTF_8), index.size(), fpp);
        for (String element : index) {
            filter.put(element);
        }
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

    private Predicate<File> isBillingFileAndMightContain(String str)
    {
        if (str.endsWith("/")) {
            str = str.substring(0, str.length() - 1);
        }
        final String searchTerm = str;
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
                    return matcher.matches() && mightContain(getIndexName(file, matcher));
                } catch (ClassNotFoundException | IOException e) {
                    throw new RuntimeException("Failed to read index", e);
                }
            }

            private File getIndexName(File file, Matcher matcher)
            {
                return new File(file.getParentFile(), matcher.replaceAll("index-$1"));
            }

            private boolean mightContain(File index)
                    throws IOException, ClassNotFoundException
            {
                return searchTerm.isEmpty() || !index.exists() ||
                        ((BloomFilter<CharSequence>) readFromFile(index)).mightContain(searchTerm);
            }
        };
    }

    /**
     * Billing file line processor that collects strings to index.
     */
    private static class IndexProcessor implements LineProcessor<Set<String>>
    {
        private final Set<String> result = Sets.newConcurrentHashSet();
        private final Function<String, String[]> parser;

        private IndexProcessor(ConfigurationProperties configuration)
                throws IOException, URISyntaxException
        {
            parser = new BillingParserBuilder(configuration)
                    .addAttribute("path")
                    .addAttribute("pnfsid")
                    .addAttribute("owner")
                    .buildToArray();
        }

        @Override
        public boolean processLine(String line) throws IOException
        {
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

    /**
     * A factory for Bzip2CompressorInputStream.
     */
    private static class Bzip2CompressorInputStreamSupplier implements InputSupplier<InputStream>
    {
        private final File file;

        public Bzip2CompressorInputStreamSupplier(File file)
        {
            this.file = file;
        }

        @Override
        public InputStream getInput() throws IOException
        {
            return new BZip2CompressorInputStream(new BufferedInputStream(new FileInputStream(file)));
        }
    }

    /**
     * A factory for Bzip2CompressorOutputStream.
     */
    private static class Bzip2CompressorOutputStreamSupplier implements OutputSupplier<OutputStream>
    {
        private final File file;

        public Bzip2CompressorOutputStreamSupplier(File file)
        {
            this.file = file;
        }

        @Override
        public OutputStream getOutput() throws IOException
        {
            return new BZip2CompressorOutputStream(new BufferedOutputStream(new FileOutputStream(file)));
        }
    }

    public static void main(String[] arguments)
            throws IOException, URISyntaxException, ExecutionException, InterruptedException,
                   ClassNotFoundException
    {
        LogManager.getLogManager().reset();
        SLF4JBridgeHandler.install();
        Thread.setDefaultUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler()
        {
            @Override
            public void uncaughtException(Thread t, Throwable e)
            {
                LOGGER.error("Uncaught exception", t);
            }
        });

        new Indexer(new Args(arguments));
    }
}
