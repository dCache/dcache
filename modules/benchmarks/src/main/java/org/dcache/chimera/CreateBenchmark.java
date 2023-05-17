package org.dcache.chimera;


import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.UUID.randomUUID;

import com.google.common.io.Resources;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;
import java.util.UUID;
import liquibase.Liquibase;
import liquibase.database.Database;
import liquibase.database.DatabaseFactory;
import liquibase.database.jvm.JdbcConnection;
import liquibase.exception.LiquibaseException;
import liquibase.resource.ClassLoaderResourceAccessor;
import org.dcache.chimera.CreateBenchmark.DB.ThreadCtx;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.slf4j.bridge.SLF4JBridgeHandler;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;

@BenchmarkMode(Mode.Throughput)
public class CreateBenchmark {

    static {
        // redirect java.util.logging used by liquibase
        SLF4JBridgeHandler.removeHandlersForRootLogger();
        SLF4JBridgeHandler.install();
    }
    private final static URL DB_TEST_PROPERTIES =
          Resources.getResource("org/dcache/chimera/chimera-benchmark.properties");

    @State(Scope.Benchmark)
    public static class DB {

        @Param(value = {"weak", "strong", "week_softupdate"})
        String wcc;

        protected HikariDataSource _dataSource;

        protected FsInode _rootInode;

        protected FileSystemProvider _fs;


        @Setup
        public void setUp() throws IOException, SQLException, LiquibaseException {

            switch (wcc) {
                case "week_softupdate":
                    System.setProperty("chimera_soft_update", "true");
                case "weak":
                    System.setProperty("chimera_lazy_wcc", "true");
                case "strong":
                    ;
                    break;
                default:
                    throw new IllegalArgumentException("Invalid wcc mode: " + wcc);
            }

            Properties dbProperties = new Properties();
            try (InputStream input = Resources.asByteSource(DB_TEST_PROPERTIES).openStream()) {
                dbProperties.load(input);
            }

            HikariConfig config = new HikariConfig();
            config.setJdbcUrl(dbProperties.getProperty("chimera.db.url"));
            config.setUsername(dbProperties.getProperty("chimera.db.user"));
            config.setPassword(dbProperties.getProperty("chimera.db.password"));
            config.setMaximumPoolSize(128);
            config.setMinimumIdle(64);
            config.setAutoCommit(true);
            config.setTransactionIsolation("TRANSACTION_READ_COMMITTED");


            _dataSource = new HikariDataSource(config);

            try (Connection conn = _dataSource.getConnection()) {

                conn.createStatement().execute("DROP SCHEMA public CASCADE;");
                conn.createStatement().execute("CREATE SCHEMA public;");
                conn.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);

                Database database = DatabaseFactory.getInstance()
                      .findCorrectDatabaseImplementation(new JdbcConnection(conn));
                Liquibase liquibase = new Liquibase(
                      "org/dcache/chimera/changelog/changelog-master.xml",
                      new ClassLoaderResourceAccessor(), database);

                liquibase.update("");
                conn.createStatement().execute("ALTER TABLE t_inodes SET (fillfactor = 50);");
            }

            PlatformTransactionManager txManager = new DataSourceTransactionManager(_dataSource);
            _fs = new JdbcFs(_dataSource, txManager);
            _rootInode = _fs.path2inode("/");
            _fs.createTag(_rootInode, "aTag");
            FsInode tagInode = new FsInode_TAG(_fs, _rootInode.ino(), "aTag");
            byte[] data = "data".getBytes(UTF_8);
            tagInode.write(0, data, 0, data.length);
        }


        @State(Scope.Thread)
        public static class ThreadCtx {
            FsInode threadRoot;

            @Setup
            public void setUp(DB db) throws ChimeraFsException, SQLException {
                threadRoot = db._fs.path2inode("/");
            }
        }

        @TearDown
        public void tearDown() throws Exception {
            _fs.close();
            _dataSource.close();
        }
    }

    @Benchmark
    @Threads(value = 64)
    public FsInode benchmarkCreateDir(ThreadCtx ctx) throws ChimeraFsException {
        var dirName = UUID.randomUUID().toString();
        FsInode sub = ctx.threadRoot.mkdir(dirName);
        return sub;
    }

    @Benchmark
    @Threads(value = 64)
    public FsInode benchmarkCreateDeleteDir(ThreadCtx ctx) throws ChimeraFsException {
        var dirName = UUID.randomUUID().toString();
        FsInode sub = ctx.threadRoot.mkdir(dirName);
        ctx.threadRoot.remove(dirName);
        return sub;
    }

    @Benchmark
    @Threads(value = 64)
    public FsInode benchmarkCreateFile(ThreadCtx ctx) throws ChimeraFsException {
        return ctx.threadRoot.create(randomUUID().toString(), 0, 0, 644);
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
              .include(CreateBenchmark.class.getSimpleName())
              .build();

        new Runner(opt).run();
    }
}
