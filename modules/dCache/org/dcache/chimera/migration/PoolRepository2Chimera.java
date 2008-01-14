package org.dcache.chimera.migration;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.dcache.chimera.DbConnectionInfo;
import org.dcache.chimera.XMLconfig;
import org.dcache.chimera.util.SqlHelper;

public class PoolRepository2Chimera {

    public static void main(String[] args) throws Exception {

        if (args.length != 2) {
            System.err
                    .println("Usage: PoolRepository2Chimera <chimera-config> <pool base dir>");
            System.exit(1);
        }


        XMLconfig config = new XMLconfig(new File(args[0]));


        DbConnectionInfo cInfo = config.getDbInfo(0);
        Class.forName(cInfo.getDBdrv());
        Connection mappingdbConnection = DriverManager.getConnection(cInfo.getDBurl(), cInfo.getDBuser(), cInfo.getDBpass());
        mappingdbConnection.setAutoCommit(true);

        File poolRoot = new File(args[1]);

        if (!poolRoot.exists() || !poolRoot.isDirectory()) {
            throw new IllegalArgumentException(args[1] + "does not exists or not a directory");
        }

        File controlDir = new File(poolRoot, "control");
        if (!controlDir.exists() || !controlDir.isDirectory()) {
            throw new IllegalArgumentException(controlDir
                    + "does not exist or not a directory");
        }

        File dataDir = new File(poolRoot, "data");
        if (!dataDir.exists() || !dataDir.isDirectory()) {
            throw new IllegalArgumentException(dataDir
                    + "does not exist or not a directory");
        }


        String[] files = dataDir.list();

        for( String id: files ) {

            String chimeraId = getIdMapping(mappingdbConnection, id);

            if(chimeraId == null ) {
                System.err.println("No Chimera-based ID found for " + id);
                continue;
            }

            File dataFile = new File(dataDir, id);
            File controlFile = new File(controlDir, id);
            File siFile = new File(controlDir, "SI-" + id);


            // FIXME: we need a transaction here
            dataFile.renameTo( new File(dataDir, chimeraId));
            controlFile.renameTo( new File(controlDir, chimeraId));
            siFile.renameTo( new File(controlDir, "SI-" + chimeraId));


        }

    }

    private static String getIdMapping(Connection mappingdbConnection, String id) throws SQLException {


        PreparedStatement ps = mappingdbConnection.prepareStatement("SELECT ichimeraid FROM t_pnfsid_mapping WHERE ipnfsid=?");
        ResultSet rs = null;
        try {

            rs = ps.executeQuery();
            if( rs.next() ) {
                return rs.getString(1);
            } else {
                return null;
            }

        }finally{
            SqlHelper.tryToClose(rs);
            SqlHelper.tryToClose(ps);
        }

    }

}
