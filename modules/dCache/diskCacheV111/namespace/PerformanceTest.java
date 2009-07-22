package diskCacheV111.namespace;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import org.dcache.auth.Subjects;

import diskCacheV111.util.PnfsId;
import diskCacheV111.namespace.provider.DcacheNameSpaceProviderFactory;
import dmg.util.Args;

/**
 * This utility performs performance tests for various name space
 * lookup operations. The utility is independent of any specific name
 * space provider and can be used with any name space provider
 * providing a DcacheNameSpaceProviderFactory.
 *
 * The utility can measure pnfsid lookup, file meta data lookup and
 * storage info lookup. It can use a configurable number of threads.
 */
public class PerformanceTest extends Thread
{
    private static NameSpaceProvider provider;
    private static BlockingQueue<String> queue;
    private static boolean lookupFileMetaData;
    private static boolean lookupStorageInfo;

    public static void main(String arguments[]) throws Exception
    {
        /* Parse arguments.
         */
        Args args = new Args(arguments);
        if (args.argc() < 2) {
            System.err.println("Usage: PerformanceTest [-filemetadata] [-storageinfo] [-threads=<n>] <file> <provider-factory> ...");
            System.err.println("   where <file> contains a list of paths to load.");
            System.err.println("Options:");
            System.err.println("  -filemetadata  Read file meta data of files");
            System.err.println("  -storageinfo   Read storage info of files (implies -filemetadata)");
            System.err.println("  -threads       Sets number of concurrent reads");
            System.err.println("");
            System.err.println("Remaining arguments are passed on to the provider factory.");
            System.exit(2);
        }

        String fileName = args.argv(0);
        String factoryName = args.argv(1);
        args.shift();
        args.shift();

        lookupFileMetaData = (args.getOpt("filemetadata") != null);
        lookupStorageInfo = (args.getOpt("storageinfo") != null);
        int concurrency =
            (args.getOpt("threads") != null) ? Integer.parseInt(args.getOpt("threads")) : 1;

        /* Instantiate provider.
         */
        DcacheNameSpaceProviderFactory factory =
            (DcacheNameSpaceProviderFactory)Class.forName(factoryName).newInstance();
        provider = (NameSpaceProvider)factory.getProvider(args, null);

        /* Read paths.
         */
        System.out.println("Loading " + fileName);
        queue = new LinkedBlockingQueue();
        int files = 0;
        BufferedReader reader = new BufferedReader(new FileReader(fileName));
        try {
            String line;
            while ((line = reader.readLine()) != null) {
                queue.put(line);
                files++;
            }
        } finally {
            reader.close();
        }

        /* Run test.
         */
        System.out.println("Running test...");
        long start = System.currentTimeMillis();
        Thread[] threads = new Thread[concurrency];
        for (int i = 0; i < concurrency; i++) {
            threads[i] = new PerformanceTest();
            threads[i].start();
        }
        for (int i = 0; i < concurrency; i++) {
            threads[i].join();
        }
        long total = System.currentTimeMillis() - start;

        /* Report result.
         */
        System.out.println("Number of files  : " + files);
        System.out.println("Number of threads: " + concurrency);
        System.out.println("Operations       : lookup pnfsid"
                           + (lookupStorageInfo
                              ? ", lookup storageinfo"
                              : (lookupFileMetaData
                                 ? ", lookup file meta data"
                                 : "")));
        System.out.println("Time             : " + total + " ms");
        System.out.println("Average pr. op   : " + (double)total/files + " ms");
    }

    public void run()
    {
        String path;
        while ( (path = queue.poll()) != null) {
            try {
                PnfsId id = provider.pathToPnfsid(Subjects.ROOT, path, true);
                if (lookupStorageInfo) {
                    provider.getStorageInfo(Subjects.ROOT, id);
                } else if (lookupFileMetaData) {
                    provider.getFileMetaData(Subjects.ROOT, id);
                }
            } catch (Exception e) {
                System.err.println(e.getMessage());
            }
        }
    }
}
