package org.dcache.chimera.migration;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.slf4j.LoggerFactory;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.encoder.PatternLayoutEncoder;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.ConsoleAppender;

import org.dcache.chimera.namespace.ChimeraNameSpaceProvider;

import diskCacheV111.namespace.NameSpaceProvider;
import diskCacheV111.namespace.provider.BasicNameSpaceProvider;
import diskCacheV111.util.PnfsFile;
import diskCacheV111.util.PnfsId;
import dmg.util.Args;

public class Comparator {

    private static final String OPTION_CHIMIERA_CONFIG = "config";
    private static final String OPTION_PNFS_MOUNT = "pnfsMount";
    private static final String PNFS_ID_DESCRIPTION = "file IDs";

    private static final String FILENAME_CHIMERA_CONFIG_DEFAULT =
            "/opt/d-cache/config/chimera-config.xml";

    private static final String LOG_PATTERN = "%-5level - %msg%n";

    private static int _errorCount = 0;

    public static void main( String args[]) throws Exception {

        Args parsedArgs = new Args( args);

        if( parsedArgs.argc() != 1 || parsedArgs.isOneCharOption( 'h')) {
            System.err.println( "Usage:");
            System.err.println( "    comparator [-k] [-v] [-h] [-" +
                                OPTION_CHIMIERA_CONFIG + "=<file>] [-" +
                                OPTION_PNFS_MOUNT + "=<dir>] <file>");
            System.err.println( "");
            System.err.println( "where:");
            System.err
                    .println( "    -k          \tcheck should continue should an inconsistency be discovered.");
            System.err.println( "    -v          \tincrease verbosity.");
            System.err.println( "    -h          \thelp.");
            System.err
                    .println( "    -" + OPTION_CHIMIERA_CONFIG +
                              "=<file>\tuse <file> for Chimera configuration.");
            System.err.println( "                \t(" +
                                FILENAME_CHIMERA_CONFIG_DEFAULT +
                                " by default)");
            System.err.println( "    -" + OPTION_PNFS_MOUNT +
                                "=<dir>\tuse <dir> as PNFS mount-point");
            System.err
                    .println( "                \t(auto-detect by default, RECOMMENDED)");
            System.err
                    .println( "    <file>      \tcontains a list of PNFS IDs of files to verify.");
            System.exit( 2);
        }

        String suppliedConfigFilename =
                parsedArgs.getOpt( OPTION_CHIMIERA_CONFIG);
        String chimeraConfigFilename =
                suppliedConfigFilename != null ? suppliedConfigFilename
                        : FILENAME_CHIMERA_CONFIG_DEFAULT;

        String pnfsMount = parsedArgs.getOpt( OPTION_PNFS_MOUNT);

        String file = parsedArgs.argv( 0);
        boolean showAllErrors = parsedArgs.isOneCharOption( 'k');

        Level level =
                parsedArgs.isOneCharOption( 'v') ? Level.DEBUG : Level.WARN;
        switchLogging( "logger.org.dcache.namespace." +
                       BasicNameSpaceProvider.class.getName(), level);
        switchLogging( "logger.dev.org.dcache.namespace." +
                       PnfsFile.class.getName(), level);
        switchLogging( "org.dcache.chimera.migration", level);

        String pnfsArgs =
                "diskCacheV111.util.GenericInfoExtractor " +
                        "-delete-registration=dummyLocation -delete-registration-jdbcDrv=foo " +
                        "-delete-registration-dbUser=dummyUser -delete-registration-dbPass=dummyPass " +
                        (pnfsMount != null ? "-pnfs=" + pnfsMount : "");
        String chimeraArgs =
                "org.dcache.chimera.namespace.ChimeraOsmStorageInfoExtractor " +
                        "-chimeraConfig=" + chimeraConfigFilename;

        PnfsIdValidator validator = newValidator( chimeraArgs, pnfsArgs);

        BufferedReader br = null;
        try {
            br = new BufferedReader( new FileReader( new File( file)));
        } catch (FileNotFoundException e1) {
            System.out.println( "\nCouldn't find file " + file);
            System.exit( 2);
        }

        final TerminableBlockingQueue<PnfsId> queue = newBlockingQueue();

        addShutdownHook( queue);

        PnfsIdProducer producer = new PnfsIdProducer( br, queue);
        producer.start();

        for( PnfsId id = queue.take(); !queue.hasTerminateWith( id); id =
                queue.take()) {

            if( !validator.isOK( id)) {
                _errorCount++;

                if( !showAllErrors)
                    queue.terminate();
            }
        }

        if( _errorCount > 0)
            System.exit( 1);
    }

    /**
     * Add a thread to the JVM shutdown-hook that terminates the queue, if
     * not already terminated and emits the error count message.
     *
     * @param queue The queue to ensure is terminated on shutdown.
     */
    private static void addShutdownHook( final TerminableBlockingQueue<PnfsId> queue) {
        Runtime runtime = Runtime.getRuntime();

        runtime.addShutdownHook( new Thread() {
            TerminableBlockingQueue<PnfsId> _queue;
            {
                _queue = queue;
            }

            @Override
            public void run() {
                if( !_queue.isTerminated())
                    _queue.terminate();

                emitErrorCount();
            }
        });
    }

    /**
     * Emit a one-line message to stdout describing the errors.
     */
    private static void emitErrorCount() {
        if( _errorCount > 0)
            System.out.println( _errorCount + " failure" +
                                (_errorCount > 1 ? "s" : "") + " discovered.");
        else
            System.out.println( "All IDs are OK.");
    }

    /**
     * Alter name named logger so it emits output to the console for log
     * messages of the given priority level or higher
     *
     * @param name the name of the Log4J logger to alter
     * @param level the minimum priority level for output to be emitted to
     *            the console.
     */
    private static void switchLogging( String name, Level level)
    {
        LoggerContext loggerContext =
            (LoggerContext) LoggerFactory.getILoggerFactory();

        ConsoleAppender<ILoggingEvent> ca =
            new ConsoleAppender<ILoggingEvent>();
        ca.setContext(loggerContext);
        ca.setName(name);
        PatternLayoutEncoder pl = new PatternLayoutEncoder();
        pl.setContext(loggerContext);
        pl.setPattern(LOG_PATTERN);
        pl.start();

        ca.setEncoder(pl);
        ca.start();

        Logger logger = loggerContext.getLogger(name);
        logger.detachAndStopAllAppenders();
        logger.addAppender(ca);
        logger.setLevel(level);
    }

    /**
     * Create a new TerminableBlockingQueue to be used by the PnfsProducer.
     */
    private static TerminableBlockingQueue<PnfsId> newBlockingQueue() {
        BlockingQueue<PnfsId> idQueue =
                new ArrayBlockingQueue<PnfsId>( 1, true);

        PnfsId sentinel = new PnfsId( "0000000000000000000000");
        TerminableBlockingQueue<PnfsId> terminableQueue =
                new TerminableBlockingQueueDecorator<PnfsId>( idQueue,
                                                              sentinel, 1);

        TerminableBlockingQueue<PnfsId> loggingQueue =
                new LoggingTerminableBlockingQueueDecorator<PnfsId>( terminableQueue,
                                                                     System.out,
                                                                     PNFS_ID_DESCRIPTION);
        return loggingQueue;
    }

    /**
     * Create a new PnfsIdValidator that checks the FileMetaData and
     * StorageInfo of the supplied PNFS-IDs.
     *
     * @throws Exception if the ChimeraNameSpaceProvider or (PNFS)
     *             BasicNameSpaceProvider throws an Exception.
     */
    private static PnfsIdValidator newValidator( String chimeraArgs,
                                                 String pnfsArgs)
            throws Exception {
        NameSpaceProvider chimeraNamespace =
                new ChimeraNameSpaceProvider( new Args( chimeraArgs), null);

        NameSpaceProvider pnfsNamespace =
                new BasicNameSpaceProvider( new Args( pnfsArgs), null);

        PnfsIdValidator checkFileMetaData =
                new FileMetaDataComparator( System.err, chimeraNamespace,
                                            pnfsNamespace);
        PnfsIdValidator checkStorageInfo =
                new StorageInfoComparator( System.err, chimeraNamespace,
                                           pnfsNamespace);
        PnfsIdValidator combinedComparator =
                new CombinedComparator( checkFileMetaData, checkStorageInfo);

        return combinedComparator;
    }

}
