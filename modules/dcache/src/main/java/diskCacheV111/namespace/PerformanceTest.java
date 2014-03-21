package diskCacheV111.namespace;

import com.google.common.base.Throwables;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import diskCacheV111.util.AccessLatency;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.PnfsId;
import diskCacheV111.util.RetentionPolicy;
import diskCacheV111.vehicles.StorageInfo;

import org.dcache.auth.Subjects;
import org.dcache.namespace.FileAttribute;
import org.dcache.util.Args;
import org.dcache.util.Checksum;
import org.dcache.util.ChecksumType;
import org.dcache.vehicles.FileAttributes;

import static org.dcache.namespace.FileAttribute.*;


enum Operation {
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
    STORAGE_INFO("storageinfo", "Read storage info of files (implies -filemetadata)");
    private final String userInput;
    private final String desc;
    Operation(String userInput, String desc) {
        this.userInput = userInput;
	this.desc = desc;
    }
    public String getDesc() {
        return desc;
    }
    public String getUserInput() {
        return userInput;
    }
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append('\t');
        sb.append('-');
        sb.append(userInput);
        sb.append('\t');
        sb.append(desc);
        return sb.toString();
    }
}

/**
 * This utility performs performance tests for various name space
 * lookup operations. The utility is independent of any specific name
 * space provider and can be used with any name space provider
 * providing a factory.
 *
 * The utility can measure pnfsid lookup, file meta data lookup and
 * storage info lookup. It can use a configurable number of threads.
 */
public class PerformanceTest extends Thread
{
    private static NameSpaceProvider provider;
    private static BlockingQueue<String> queue;
    private static List<Operation> ops;
    private static final String CACHE_LOCATION = "myPoolD";
    private static final int UID = 0;
    private static final int GID = 0;
    private static final int PERMISSION = 777;
    private static final Checksum CHECKSUM =
        new Checksum(ChecksumType.ADLER32, "123456");

    public static List<String> getPaths(String fileName) throws IOException {
        List<String> toReturn = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(fileName))) {
            String line;
            while ((line = reader.readLine()) != null) {
                toReturn.add(line);
            }
        }

        return toReturn;
    }

    public static List<Operation> getOps(Args args){
        List<Operation> toReturn = new ArrayList<>();
        for(Operation aOp: Operation.values()) {
            if(args.hasOption(aOp.getUserInput())) {
		    toReturn.add(aOp);
	    }
        }
        return toReturn;
    }

    public static void main(String arguments[]) throws Exception
    {
        /* Parse arguments.
         */
        Args args = new Args(arguments);
        if (args.argc() < 2) {
            System.err.print("Usage: PerformanceTest [-threads=<n>] ");
            for(Operation aOp: Operation.values()) {
		    System.err.print("[-" + aOp.getUserInput() + "] ");
	    }
            System.err.println(" <file> <provider-factory>");
            System.err.println("  where <file> contains a list of paths to load.");
            System.err.println("  and   <provider-factory> is of the type DcacheNameSpaceProviderFactory");
            System.err.println("Options:");
            for(Operation aOp: Operation.values()) {
		    System.err.println(aOp);
	    }
            System.err.println("\t-threads\tSets number of concurrent reads");
            System.err.println("");
            System.err.println("Remaining arguments are passed on to the provider factory.");
            System.exit(2);
        }

        String fileName = args.argv(0);
        String factoryName = args.argv(1);
        args.shift();
        args.shift();

	ops = getOps(args);
        int concurrency =
            (args.hasOption("threads")) ? Integer.parseInt(args.getOpt("threads")) : 1;

        /* Instantiate provider.
         */
        Class<?> factory = Class.forName(factoryName);
        Method factoryMethod = factory.getMethod("getProvider", Args.class);
        provider = (NameSpaceProvider) factoryMethod.invoke(null, args);

        /* Read paths.
         */
        System.out.println("Loading " + fileName);
        queue = new LinkedBlockingQueue<>();
        List<String> paths = getPaths(fileName);
        queue.addAll(paths);


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
        long end = System.currentTimeMillis();
        long total = end - start;

        /* Report result.
         */
        System.out.println("Number of files  : " + paths.size());
        System.out.println("Number of threads: " + concurrency);
        System.out.print("Operations       : [");
        for(Operation aOp: ops) {
            System.out.print(aOp.getUserInput() + ", ");
        }
        System.out.println("]");

        System.out.println("Start time       : " + new Date(start));
        System.out.println("End time         : " + new Date(end));
        System.out.println("Total time       : " + total + " ms");
        System.out.println("Average pr. op   : " + (double)(total)/paths.size() + " ms");


    }

    private PnfsId getPnfsid(String path) throws CacheException {
        return provider.pathToPnfsid(Subjects.ROOT, path, true);
    }

    private void processOperation(Operation aOp, String path) throws URISyntaxException
    {
        try {
            FileAttributes fileAttributes;
            switch (aOp) {
                case CREATE_ENTRY:
                    provider.createFile(Subjects.ROOT, path, UID, GID, PERMISSION, EnumSet.noneOf(FileAttribute.class));
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
                    provider.deleteEntry(Subjects.ROOT, getPnfsid(path));
                    break;
                case PNFS_ID_TO_PATH:
                    provider.pnfsidToPath(Subjects.ROOT, getPnfsid(path));
                    break;
                case GET_PARENT:
                    provider.getParentOf(Subjects.ROOT, getPnfsid(path));
                    break;
                case ADD_CHECKSUM:
                    fileAttributes = new FileAttributes();
                    fileAttributes.setChecksums(Collections.singleton(CHECKSUM));
                    provider.setFileAttributes(Subjects.ROOT, getPnfsid(path),
                            fileAttributes, EnumSet.noneOf(FileAttribute.class));
                    break;
                case GET_CHECKSUMS:
                    Set<Checksum> cksums = provider.getFileAttributes(Subjects.ROOT, getPnfsid(path), EnumSet.of(FileAttribute.CHECKSUM)).getChecksums();
                    break;
                case SET_FILE_ATTR:
                    fileAttributes = new FileAttributes();
                    fileAttributes.setAccessLatency(AccessLatency.ONLINE);
                    fileAttributes.setRetentionPolicy(RetentionPolicy.REPLICA);
                    provider.setFileAttributes(Subjects.ROOT, getPnfsid(path),
                            fileAttributes, EnumSet.noneOf(FileAttribute.class));
                    break;
                case GET_FILE_ATTR:
                    provider.getFileAttributes(Subjects.ROOT, getPnfsid(path), EnumSet.of(FileAttribute.FLAGS));
                    break;
                case ADD_CACHE_LOC:
                    provider.addCacheLocation(Subjects.ROOT, getPnfsid(path), CACHE_LOCATION);
                    break;
                case GET_CACHE_LOC:
                    List<String> loc = provider.getCacheLocation(Subjects.ROOT, getPnfsid(path));
                    break;
		case STORAGE_INFO:
                    StorageInfo info = provider.getFileAttributes(Subjects.ROOT, getPnfsid(path), EnumSet.of(FileAttribute.STORAGEINFO)).getStorageInfo();
                    break;
                case SET_STORAGE_INFO:
                    info = provider.getFileAttributes(Subjects.ROOT, getPnfsid(path), EnumSet.of(FileAttribute.STORAGEINFO)).getStorageInfo();
                    info.setHsm("hsm");
                    info.setKey("store", "test");
                    info.setKey("group", "disk");
                    info.addLocation(new URI("osm://hsm/?store=test&group=disk&bdif=1234"));
                    FileAttributes attributesToUpdate = new FileAttributes();
                    attributesToUpdate.setStorageInfo(info);
                    provider.setFileAttributes(Subjects.ROOT, getPnfsid(path),
                            attributesToUpdate, EnumSet.noneOf(FileAttribute.class));
                    break;
                default: break;
            }//switch
        }catch (CacheException e) {
            System.err.println("Exception " + aOp.getUserInput() + " :" + e.getMessage());
        }
    }

    @Override
    public void run() {
        try {
            String path;
            while ( (path = queue.poll()) != null) {
                for(Operation aOp: ops) {
                    processOperation(aOp, path);
                }
            }
        } catch (URISyntaxException e) {
            Throwables.propagate(e);
        }
    }//run
}//class
