import java.io.*;
import java.sql.*;
import java.text.*;

/**
 * Class PlotSettings builds the list of the settings for the plot.
 */
public class PlotSettings
{
    /**
     * Class constructor. Executes simple query for the given table and fills the setting list
     */
    public PlotSettings(Connection conn, String tableName)
        throws SQLException
    {
        Statement stmt = conn.createStatement();	// Our statement to run queries with
        stmt.close();
        ResultSet rset = stmt.executeQuery("select * from " + tableName);
        if (rset != null) {
            int rs = rset.getFetchSize();
            _settings = new String[rs];
            for (int i = 0; rset.next(); i++) {
                _settings[i] = rset.getString(1);
            }
        }
        rset.close(); // again, you must close the result when done
    }

    /**
     * Returns the reference to the settings
     */
    public String[] getSettings()
    {
        return _settings;
    }

    /**
     * Auxilary method for debug. Dumps settings in readable format.
     */
    public String dump()
    {
        StringBuffer sDump = new StringBuffer("[");
        for (int i = 0; i < _settings.length; i++) {
            sDump.append("\""+_settings[i]+"\" ");
        }
        sDump.append("]");
        return sDump.toString();
    }

    private String[] _settings = null;
}
