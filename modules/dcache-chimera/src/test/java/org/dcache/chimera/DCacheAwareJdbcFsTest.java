package org.dcache.chimera;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.io.Resources;
import com.zaxxer.hikari.HikariDataSource;
import diskCacheV111.util.CacheException;
import java.io.InputStream;
import java.net.URL;
import java.sql.Connection;
import java.util.Properties;
import liquibase.Liquibase;
import liquibase.database.Database;
import liquibase.database.DatabaseFactory;
import liquibase.database.jvm.JdbcConnection;
import liquibase.resource.ClassLoaderResourceAccessor;
import org.dcache.space.Capacity;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;

public class DCacheAwareJdbcFsTest {

    private static final URL DB_TEST_PROPERTIES =
          Resources.getResource("org/dcache/chimera/chimera-test.properties");

    private HikariDataSource dataSource;
    private DCacheAwareJdbcFs fs;
    private FsStat namespaceStat;

    @Before
    public void setUp() throws Exception {
        Properties dbProperties = new Properties();
        try (InputStream input = Resources.asByteSource(DB_TEST_PROPERTIES).openStream()) {
            dbProperties.load(input);
        }

        dataSource = FsFactory.getDataSource(
              dbProperties.getProperty("chimera.db.url"),
              dbProperties.getProperty("chimera.db.user"),
              dbProperties.getProperty("chimera.db.password"));

        try (Connection conn = dataSource.getConnection()) {
            Database database = DatabaseFactory.getInstance()
                  .findCorrectDatabaseImplementation(new JdbcConnection(conn));
            Liquibase liquibase = new Liquibase("org/dcache/chimera/changelog/changelog-master.xml",
                  new ClassLoaderResourceAccessor(), database);
            liquibase.update("");
        }

        fs = new DCacheAwareJdbcFs(dataSource, new DataSourceTransactionManager(dataSource),
              "strong");
        namespaceStat = fs.getFsStat();
    }

    @After
    public void tearDown() throws Exception {
        try (Connection conn = dataSource.getConnection()) {
            conn.createStatement().execute("SHUTDOWN;");
        }
        dataSource.close();
        fs.close();
    }

    @Test
    public void shouldReportNamespaceStatisticsByDefault() throws Exception {
        assertEquals(JdbcFs.AVAILABLE_SPACE, fs.getFsStat().getTotalSpace());
    }

    @Test
    public void shouldReportCapacityOfProvider() throws Exception {
        Capacity.Provider provider = provider(Capacity.Source.POOLS);
        when(provider.get()).thenReturn(Capacity.UNKNOWN);
        fs.setCapacity(provider);

        FsStat stat = fs.getFsStat();

        assertEquals(1L << 60, stat.getTotalSpace());
        assertEquals(0, stat.getUsedSpace());
        assertEquals(namespaceStat.getUsedFiles(), stat.getUsedFiles());
    }

    @Test
    public void shouldFallBackToNamespaceStatistics() throws Exception {
        Capacity.Provider provider = provider(Capacity.Source.POOLS);
        when(provider.get()).thenThrow(new CacheException("Pool information is not available"));
        fs.setCapacity(provider);

        assertEquals(namespaceStat.getUsedSpace(), fs.getFsStat().getUsedSpace());
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldRejectReservationOfWriteToken() {
        Capacity.Provider provider = provider(Capacity.Source.RESERVATION);
        when(provider.usesWriteToken()).thenReturn(true);
        fs.setCapacity(provider);
    }

    private static Capacity.Provider provider(Capacity.Source source) {
        Capacity.Provider provider = mock(Capacity.Provider.class);
        when(provider.getSource()).thenReturn(source);
        return provider;
    }
}
