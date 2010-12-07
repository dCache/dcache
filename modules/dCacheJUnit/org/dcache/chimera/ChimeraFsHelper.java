package org.dcache.chimera;

import com.mchange.v2.c3p0.DataSources;
import java.io.IOException;
import java.sql.SQLException;
import javax.sql.DataSource;

public class ChimeraFsHelper {

    private ChimeraFsHelper() {};

    public static FileSystemProvider getFileSystemProvider(String url, String drv, String user,
            String pass, String dialect)
            throws IOException, ClassNotFoundException, SQLException {

        Class.forName(drv);

        DataSource dataSource = DataSources.unpooledDataSource(url,
                user, pass);

        return new JdbcFs(DataSources.pooledDataSource(dataSource), dialect);
    }
}
