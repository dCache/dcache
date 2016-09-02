/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2015 Deutsches Elektronen-Synchrotron
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
package org.dcache.chimera;

import com.google.common.base.Stopwatch;
import com.google.common.base.Throwables;
import com.zaxxer.hikari.HikariDataSource;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import diskCacheV111.util.AccessLatency;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.PnfsId;
import diskCacheV111.util.RetentionPolicy;
import diskCacheV111.vehicles.StorageInfo;

import org.dcache.auth.Subjects;
import org.dcache.chimera.namespace.ChimeraNameSpaceProvider;
import org.dcache.chimera.namespace.ChimeraOsmStorageInfoExtractor;
import org.dcache.namespace.FileAttribute;
import org.dcache.namespace.FileType;
import org.dcache.namespace.PosixPermissionHandler;
import org.dcache.util.Args;
import org.dcache.util.Checksum;
import org.dcache.util.ChecksumType;
import org.dcache.vehicles.FileAttributes;

import static diskCacheV111.util.AccessLatency.ONLINE;
import static diskCacheV111.util.RetentionPolicy.REPLICA;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static org.dcache.namespace.FileAttribute.*;

/**
 * This utility performs performance tests for various name space
 * lookup operations. The utility is independent of any specific name
 * space provider and can be used with any name space provider
 * providing a factory.
 * <p>
 * The utility can measure pnfsid lookup, file meta data lookup and
 * storage info lookup. It can use a configurable number of threads.
 */
public class PerformanceTest extends Thread
{
    private enum Operation
    {
        PATH_TO_PNFS_ID("pathtopnfsid", "Reads the pnfs id of a file"),
        FILE_META_DATA("filemetadata", "Reads the meta data of the file"),
        CREATE_ENTRY("createentry", "Create a new file entry in the pool"),
        DELETE_ENTRY("deleteentry", "Removes file entry from the pool"),
        PNFS_ID_TO_PATH("pnfsidtopath", "Reads path of the file"),
        GET_PARENT("getparent", "Reads the parent pnfsid of the file"),
        ADD_CHECKSUM("addchecksum", "Adds the given checksum to the file"),
        GET_CHECKSUMS("getchecksums", "Reads all the checksums of the file"),
        SET_FILE_ATTR("setfileattr", "Updates the file attributes"),
        GET_FILE_ATTR("getfileattr", "Reads the attributes of the file"),
        ADD_CACHE_LOC("addcacheloc", "Add a new pool to the file"),
        GET_CACHE_LOC("getcacheloc", "Reads all the pools of the file"),
        SET_STORAGE_INFO("setstorageinfo", "Updates the storage info of the file"),
        STORAGE_INFO("storageinfo", "Read storage info of files (implies -filemetadata)"),
        MKDIR("mkdir", "Make directory"),
        RMDIR("rmdir", "Remove directory");
        private final String userInput;
        private final String desc;

        Operation(String userInput, String desc)
        {
            this.userInput = userInput;
            this.desc = desc;
        }

        public String getDesc()
        {
            return desc;
        }

        public String getUserInput()
        {
            return userInput;
        }

        @Override
        public String toString()
        {
            return "\t" + '-' + userInput + '\t' + desc;
        }

        public static Operation find(String s)
        {
            for (Operation operation : Operation.values()) {
                if (operation.getUserInput().equals(s)) {
                    return operation;
                }
            }
            return null;
        }
    }

    private static TransactionTemplate tx;
    private static ChimeraNameSpaceProvider provider;
    private static BlockingQueue<String> queue;
    private static List<Operation> ops;
    private static final String CACHE_LOCATION = "myPoolD";
    private static final int UID = 0;
    private static final int GID = 0;
    private static final Checksum CHECKSUM =
            new Checksum(ChecksumType.ADLER32, "123456");

    public static List<String> getPaths(String fileName) throws IOException
    {
        List<String> toReturn = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(fileName))) {
            String line;
            while ((line = reader.readLine()) != null) {
                toReturn.add(line);
            }
        }

        return toReturn;
    }

    public static List<Operation> getOps(Args args)
    {
        return args.options().keys().stream().map(Operation::find).filter(Objects::nonNull).collect(toList());
    }

    public static void main(String arguments[]) throws Exception
    {
        /* Parse arguments.
         */
        Args args = new Args(arguments);
        if (args.argc() < 5) {
            System.err.print("Usage: nsp-performance.sh [-threads=<n>] ");
            for (Operation aOp : Operation.values()) {
                System.err.print("[-" + aOp.getUserInput() + "] ");
            }
            System.err.println(" <file>");
            System.err.println("  where <file> contains a list of paths to load and the remaining ");
            System.err.println("  parameters are the Chimera connection details.");
            System.err.println("Options:");
            for (Operation aOp : Operation.values()) {
                System.err.println(aOp);
            }
            System.err.println("\t-threads\tSets number of concurrent reads");
            System.err.println("\t-delay\tSets delay in seconds between progress updates");
            System.err.println("");
            System.err.println("Remaining arguments are passed on to the provider factory.");
            System.exit(2);
        }

        String jdbc = args.argv(0);
        String user = args.argv(1);
        String password = args.argv(2);
        String fileName = args.argv(3);
        args.shift(5);

        ops = getOps(args);
        int concurrency =
                (args.hasOption("threads")) ? Integer.parseInt(args.getOpt("threads")) : 1;
        int delay =
                (args.hasOption("delay")) ? Integer.parseInt(args.getOpt("delay")) : 10;

        /* Instantiate provider.
         */
        System.out.println("Starting chimera... ");
        HikariDataSource dataSource = FsFactory.getDataSource(jdbc, user, password);
        PlatformTransactionManager txManager = new DataSourceTransactionManager(dataSource);
        FileSystemProvider fileSystem = new JdbcFs(dataSource, txManager);
        provider = new ChimeraNameSpaceProvider();
        provider.setAclEnabled(false);
        provider.setExtractor(new ChimeraOsmStorageInfoExtractor(StorageInfo.DEFAULT_ACCESS_LATENCY,
                                                                 StorageInfo.DEFAULT_RETENTION_POLICY));
        provider.setFileSystem(fileSystem);
        provider.setInheritFileOwnership(false);
        provider.setPermissionHandler(new PosixPermissionHandler());
        provider.setUploadDirectory("/upload");
        provider.setVerifyAllLookups(true);
        tx = new TransactionTemplate(txManager);

        /* Read paths.
         */
        System.out.println("Loading " + fileName);
        queue = new LinkedBlockingQueue<>();
        List<String> paths = getPaths(fileName);
        queue.addAll(paths);


        /* Run test.
         */
        System.out.println("Running test...");
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        ExecutorService executor = Executors.newCachedThreadPool();
        ScheduledFuture<?> progressTask =
                scheduler.scheduleAtFixedRate(new ProgressTask(), delay, delay, TimeUnit.SECONDS);
        Stopwatch watch = Stopwatch.createStarted();
        for (int i = 0; i < concurrency; i++) {
            executor.execute(new PerformanceTest());
        }
        executor.shutdown();
        executor.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
        watch.stop();
        progressTask.cancel(false);
        scheduler.shutdownNow();

        /* Report result.
         */
        StringWriter info = new StringWriter();
        provider.getInfo(new PrintWriter(info));
        System.out.println();
        System.out.println(info);
        System.out.println("Number of files  : " + paths.size());
        System.out.println("Number of threads: " + concurrency);
        System.out.println("Operations       : " + ops.stream().map(Operation::getUserInput).collect(joining(",")));
        System.out.println("Total time       : " + watch);
        System.out.println("Average pr. op   : " + ((double) watch.elapsed(TimeUnit.MICROSECONDS)) / paths.size() + " µs");
        System.out.println("Frequency        : " + 1000 * paths.size() / watch.elapsed(TimeUnit.MILLISECONDS) + " Hz");

        fileSystem.close();
        dataSource.close();
    }

    private PnfsId getPnfsid(String path) throws CacheException
    {
        return provider.pathToPnfsid(Subjects.ROOT, path, true);
    }

    private void processOperation(Operation aOp, String path)
    {
        try {
            FileAttributes fileAttributes;
            switch (aOp) {
            case CREATE_ENTRY:
                provider.createFile(Subjects.ROOT, path, UID, GID, 0664, EnumSet.noneOf(FileAttribute.class));
                break;
            case PATH_TO_PNFS_ID:
                getPnfsid(path);
                break;
            case FILE_META_DATA:
                provider.getFileAttributes(Subjects.ROOT, getPnfsid(path),
                                           EnumSet.of(OWNER, OWNER_GROUP, MODE, TYPE, SIZE,
                                                      CREATION_TIME, ACCESS_TIME, MODIFICATION_TIME, CHANGE_TIME));
                break;
            case DELETE_ENTRY:
                provider.deleteEntry(Subjects.ROOT, EnumSet.allOf(FileType.class),
                        path, EnumSet.noneOf(FileAttribute.class));
                break;
            case PNFS_ID_TO_PATH:
                provider.pnfsidToPath(Subjects.ROOT, getPnfsid(path));
                break;
            case GET_PARENT:
                provider.getParentOf(Subjects.ROOT, getPnfsid(path));
                break;
            case ADD_CHECKSUM:
                provider.setFileAttributes(Subjects.ROOT, getPnfsid(path),
                        FileAttributes.ofChecksum(CHECKSUM),
                        EnumSet.noneOf(FileAttribute.class));
                break;
            case GET_CHECKSUMS:
                provider.getFileAttributes(Subjects.ROOT, getPnfsid(path),
                                           EnumSet.of(FileAttribute.CHECKSUM)).getChecksums();
                break;
            case SET_FILE_ATTR:
                provider.setFileAttributes(Subjects.ROOT, getPnfsid(path),
                        FileAttributes.of().accessLatency(ONLINE).retentionPolicy(REPLICA).build(),
                        EnumSet.noneOf(FileAttribute.class));
                break;
            case GET_FILE_ATTR:
                provider.getFileAttributes(Subjects.ROOT, getPnfsid(path), EnumSet.allOf(FileAttribute.class));
                break;
            case ADD_CACHE_LOC:
                provider.addCacheLocation(Subjects.ROOT, getPnfsid(path), CACHE_LOCATION);
                break;
            case GET_CACHE_LOC:
                provider.getCacheLocation(Subjects.ROOT, getPnfsid(path));
                break;
            case STORAGE_INFO:
                provider.getFileAttributes(Subjects.ROOT, getPnfsid(path),
                                           EnumSet.of(FileAttribute.STORAGEINFO)).getStorageInfo();
                break;
            case SET_STORAGE_INFO:
                StorageInfo info = provider.getFileAttributes(Subjects.ROOT, getPnfsid(path),
                                                              EnumSet.of(FileAttribute.STORAGEINFO)).getStorageInfo();
                info.setHsm("hsm");
                info.setKey("store", "test");
                info.setKey("group", "disk");
                info.addLocation(new URI("osm://hsm/?store=test&group=disk&bdif=1234"));
                provider.setFileAttributes(Subjects.ROOT, getPnfsid(path),
                        FileAttributes.ofStorageInfo(info), EnumSet.noneOf(FileAttribute.class));
                break;
            case MKDIR:
                provider.createDirectory(Subjects.ROOT, path, UID, GID, 0755);
                break;
            case RMDIR:
                provider.deleteEntry(Subjects.ROOT, EnumSet.of(FileType.DIR),
                        path, EnumSet.noneOf(FileAttribute.class));
                break;
            default:
                break;
            }
        } catch (CacheException e) {
            System.err.println("Exception " + aOp.getUserInput() + " :" + e.getMessage());
        } catch (URISyntaxException e) {
            Throwables.propagate(e);
        }
    }

    @Override
    public void run()
    {
        String path;
        while ((path = queue.poll()) != null) {
            processOperation(path);
        }
    }

    private void processOperation(String path)
    {
        tx.execute(status -> {
            for (Operation aOp : ops) {
                processOperation(aOp, path);
            }
            return null;
        });
    }

    private static class ProgressTask implements Runnable
    {
        long length = queue.size();
        long time = System.currentTimeMillis();

        @Override
        public void run()
        {
            long now = System.currentTimeMillis();
            long currentLength = queue.size();
            System.out.println(String.format("Files left: %,8d  Throughput: %5d Hz", currentLength,+ 1000 * (length - currentLength) / (now - time)));
        }
    }
}
