package org.dcache.pool.repository;

import java.io.File;
import java.util.Collection;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

import diskCacheV111.util.PnfsId;

import org.slf4j.LoggerFactory;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.encoder.PatternLayoutEncoder;
import ch.qos.logback.core.ConsoleAppender;
import ch.qos.logback.classic.spi.ILoggingEvent;

public class MetaDataStoreCopyTool
{
    private final static Logger _log =
        (Logger) LoggerFactory.getLogger(MetaDataStoreCopyTool.class);

    static MetaDataStore createStore(Class<?> clazz,
                                     FileStore fileStore, File poolDir)
        throws NoSuchMethodException, InstantiationException,
               IllegalAccessException, InvocationTargetException
    {
        Constructor<?> constructor =
            clazz.getConstructor(FileStore.class, File.class);
        return (MetaDataStore) constructor.newInstance(fileStore, poolDir);
    }

    // TODO: Consider externalizing the logging configuration
    static void initLogging()
    {
        LoggerContext loggerContext =
            (LoggerContext) LoggerFactory.getILoggerFactory();
        loggerContext.reset();

        ConsoleAppender<ILoggingEvent> ca =
            new ConsoleAppender<ILoggingEvent>();
        ca.setTarget("System.out");
        ca.setContext(loggerContext);
        ca.setName("console");
        PatternLayoutEncoder pl = new PatternLayoutEncoder();
        pl.setContext(loggerContext);
        pl.setPattern("%-5level - %msg%n");
        pl.start();

        ca.setEncoder(pl);
        ca.start();
        Logger rootLogger = loggerContext.getLogger(Logger.ROOT_LOGGER_NAME);
        rootLogger.addAppender(ca);
        rootLogger.setLevel(Level.INFO);
    }

    public static void main(String[] args)
        throws Exception
    {
        if (args.length != 3) {
            System.err.println("Synopsis: MetaDataStoreCopyTool DIR FROM TO");
            System.err.println();
            System.err.println("Where DIR is the pool directory, and FROM and TO are");
            System.err.println("meta data store class names.");
            System.exit(1);
        }

        initLogging();

        File poolDir = new File(args[0]);
        FileStore fileStore = new FlatFileStore(poolDir);
        MetaDataStore fromStore =
            createStore(Class.forName(args[1]), fileStore, poolDir);
        MetaDataStore toStore =
            createStore(Class.forName(args[2]), fileStore, poolDir);

        if (!toStore.list().isEmpty()) {
            System.err.println("ERROR: Target store is not empty");
            System.exit(1);
        }

        Collection<PnfsId> ids = fromStore.list();
        int size = ids.size();
        int count = 1;
        for (PnfsId id: ids) {
            _log.info("Copying {} ({} of {})",
                      new Object[] { id, count, size });
            toStore.create(fromStore.get(id));
            count++;
        }
    }
}