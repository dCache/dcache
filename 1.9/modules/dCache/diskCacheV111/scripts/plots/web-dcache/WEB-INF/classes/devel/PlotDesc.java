import java.io.*;
import java.sql.*;
import java.text.*;


/**
 * Class PlotDesc builds one plot according to the description in SQL query result set.
 */
public class PlotDesc
{
    public PlotDesc(final ResultSet rset)
        throws SQLException
    {
        Connection conn = rset.getStatement().getConnection();
        _name   = rset.getString("name");
        if (_name == null) {
            throw new SQLException("insufficient plot info");
        }
        _gtitle = rset.getString("gtitle");
        _xlabel = rset.getString("xlabel");
        _ylabel = rset.getString("ylabel");
        String settings = rset.getString("settings");
        if (settings == null) {
            throw new SQLException("insufficient plot info");
        }
        _settings = new PlotSettings(conn, settings);
        String dataSource = rset.getString("dataSource");
        if (dataSource == null) {
            throw new SQLException("insufficient plot info");
        }
        _dataSource = new PlotDataSource(conn, dataSource);
        _ref = rset.getString("ref");
        _viewlevel = rset.getInt("viewlevel");
    }

    public void buildPlot()
    {
        _built = true;
        ;;;
    }

    public void outPlot()
    {
        ;;;
    }

    protected String getGtitle()
    {
        return _gtitle;
    }

    protected String getXlabel()
    {
        return _xlabel;
    }

    protected String getYlabel()
    {
        return _ylabel;
    }

    public void dump()
    {
        System.out.println("Plot:'"+_name+"'");
        System.out.print("  gtitle='"+_gtitle+"'");
        System.out.print("  xlabel='"+_xlabel+"'");
        System.out.println("  ylabel='"+_ylabel+"'");

        System.out.println("  settings="+_settings.dump());
        System.out.println("  dataSource="+_dataSource.dump());

        System.out.print("  ref='"+_ref+"'");
        System.out.println("  viewlevel='"+_viewlevel+"'");
    }


    private String _name = null;
    private String _gtitle = null;
    private String _xlabel = null;
    private String _ylabel = null;
    private PlotSettings _settings = null;
    private PlotDataSource _dataSource = null;
    private String _ref = null;
    private int _viewlevel = 0;
    private boolean _built = false;
}
