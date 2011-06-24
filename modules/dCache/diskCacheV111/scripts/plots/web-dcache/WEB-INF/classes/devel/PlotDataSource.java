import java.io.*;
import java.sql.*;
import java.text.*;

/**
 * Class PlotDataSource builds the list of the datasources for the plot.
 */
public class PlotDataSource
{
    /**
     * Class constructor. Executes simple query for the given table and fills the datasource list
     */
    public PlotDataSource(Connection conn, String tableName)
        throws SQLException
    {
        Statement stmt = conn.createStatement();	// Our statement to run queries with
        stmt.close();
        ResultSet rset = stmt.executeQuery("select * from " + tableName);
        if (rset != null) {
            int rs = rset.getFetchSize();
            _datasource = new String[rs][2];
            for (int i = 0; rset.next(); i++) {
                _datasource[i][0] = rset.getString(1);
                _datasource[i][1] = rset.getString(2);
            }
        }
        rset.close(); // again, you must close the result when done
    }

    /**
     * Returns the reference to the settings
     */
    public String[][] getDataSource()
    {
        return _datasource;
    }

    /**
     * Auxilary method for debug. Dumps datasources in readable format.
     */
    public String dump()
    {
        StringBuffer sDump = new StringBuffer("[");
        for (int i = 0; i < _datasource.length; i++) {
            sDump.append("\"["+_datasource[i][0]+","+_datasource[i][1]+"]\" ");
        }
        sDump.append("]");
        return sDump.toString();
    }

    private String[][] _datasource;
}
