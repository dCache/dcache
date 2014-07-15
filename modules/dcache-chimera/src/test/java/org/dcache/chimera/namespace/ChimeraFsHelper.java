package org.dcache.chimera.namespace;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import org.dcache.chimera.JdbcFs;
import org.dcache.db.AlarmEnabledDataSource;

public class ChimeraFsHelper {

    private ChimeraFsHelper() {}

    public static JdbcFs getFileSystemProvider(String url, String user,
            String pass, String dialect)
    {
        HikariConfig config = new HikariConfig();
        config.setDataSource(new DriverManagerDataSource(url, user, pass));
        config.setMinimumIdle(1);
        config.setMaximumPoolSize(2);
        return new JdbcFs(new AlarmEnabledDataSource(url,
                                                     ChimeraFsHelper.class.getSimpleName(),
                                                     new HikariDataSource(config)),
                                                     dialect);
    }
}
