import java.io.*;
import java.sql.*;
import java.text.*;

/*
 *
 * $Id: PlotTest.java,v 1.1 2003-08-27 16:49:52 cvs Exp $
 */

public class PlotTest
{
    Connection conn;	// The connection to the database
    Statement stmt;	// Our statement to run the queries

    public PlotTest(String args[]) throws ClassNotFoundException, FileNotFoundException, IOException, SQLException
    {
        String url = args[0];
        String usr = args[1];
        String pwd = args[2];

        // Load the driver
        Class.forName("org.postgresql.Driver");

        // Connect to database
        System.out.println("Connecting to Database URL = " + url);
        conn = DriverManager.getConnection(url, usr, pwd);

//         printDbMetaData(conn);

        System.out.println("Connected...Now creating a statement");
        stmt = conn.createStatement();

        // Clean up the database (in case we failed earlier) then initialise
        cleanup();

        // Now run tests using JDBC methods
        doexample();

        // Clean up the database
        cleanup();

        // Finally close the database
        System.out.println("Now closing the connection");
        stmt.close();
        conn.close();

        //throw postgresql.Driver.notImplemented();
    }

    /*
     * This drops the table (if it existed). No errors are reported.
     */
    public void cleanup()
    {
//         try {
//             stmt.executeUpdate("drop table basic");
//         }
//         catch (Exception ex) {
//             // We ignore any errors here
//         }
    }

    /**
     * Get and print DB metadata
     */
    private void printDbMetaData(Connection conn)
        throws SQLException
    {
        DatabaseMetaData dbmd = conn.getMetaData();
        ResultSet rset = dbmd.getSchemas();
        if (rset != null)	{
            while (rset.next()) {
                String sc = rset.getString(1);	// This shows how to get the value by column
                System.out.println("schema=" + sc);
            }
        }
        rset.close(); // again, you must close the result when done
        
        rset = dbmd.getCatalogs();
        if (rset != null)	{
            while (rset.next()) {
                String sc = rset.getString(1);	// This shows how to get the value by column
                System.out.println("catalog=" + sc);
            }
        }
        rset.close(); // again, you must close the result when done
    }

    /**
     * Get and print metadata
     */
    private void printMetaData(final ResultSet resultSet)
        throws SQLException
    {
        Object xObject = null;
        int columnTypes[] = null;
        // Meta data about the result set.
        ResultSetMetaData metaData;
        int column = 0;
        int numberOfColumns = 0;
        int numberOfValidColumns = 0;

        metaData = resultSet.getMetaData();
        numberOfColumns = metaData.getColumnCount();
        System.out.println("numberOfColumns="+numberOfColumns);
        columnTypes = new int[numberOfColumns];
        for (column = 0; column < numberOfColumns; column++) {
            try {
                int type = metaData.getColumnType(column + 1);
                switch (type) {
					
                case Types.NUMERIC:
                case Types.REAL:
                case Types.INTEGER:
                case Types.DOUBLE:
                case Types.FLOAT:
                case Types.BIT:
                case Types.DATE:
                case Types.TIMESTAMP:
                case Types.VARCHAR:
                    ++numberOfValidColumns;
                    columnTypes[column] = type;
                    break;
                default:
                    System.err.println("Unable to load column "
                                       + column + "(" + type + ")");
                    columnTypes[column] = Types.NULL ;
                    break;
                }
            }
            catch (SQLException e) {
                e.printStackTrace();
                columnTypes[column] = Types.NULL;
            }
            System.out.print("getColumnName("+column+")="+metaData.getColumnName(column+1));
//             System.out.print(", getColumnLabel("+column+")="+metaData.getColumnLabel(column+1));
            System.out.print(", getColumnTypeName("+column+")="+metaData.getColumnTypeName(column+1));
//             System.out.print(", getCatalogName("+column+")="+metaData.getCatalogName(column+1));
            System.out.print(", getSchemaName("+column+")="+metaData.getSchemaName(column+1));

            System.out.println(", columnTypes["+column+"]="+columnTypes[column]);
        }
    }

    /*
     * This performs the example
     */
    public void doexample() throws SQLException
    {
        System.out.println("\nRunning tests:");

        ResultSet rset = stmt.executeQuery("select * from plots");
        if (rset != null) {
//             printMetaData(rset);
            int rs = rset.getFetchSize();
            PlotDesc[] plots = new PlotDesc[rs];
            for (int i = 0; rset.next(); i++) {
                plots[i] = new PlotDesc(rset);
                plots[i].dump();
            }
        }
        rset.close(); // again, you must close the result when done

        // The last thing to do is to drop the table. This is done in the
        // cleanup() method.
    }

    /*
     * Display some instructions on how to run the example
     */
    public static void instructions()
    {
        System.out.println("\nThis example tests the basic components of the JDBC driver, demonstrating\nhow to build simple queries in java.");
        System.out.println("Usage:\n java example.basic jdbc:postgresql:database user password [debug]");
        System.out.println("The debug field can be anything. It's presence will enable DriverManager's debug trace.");
        System.out.println("Unless you want to see screens of items, don't put anything in here.");
        System.exit(1);
    }

    /*
     * This little lot starts the test
     */
    public static void main(String args[])
    {
//         System.out.println("PostgreSQL basic test v6.3 rev 1\n");

        if (args.length < 3)
            instructions();

        // This line outputs debug information to stderr. To enable this, simply
        // add an extra parameter to the command line
        if (args.length > 3)
            DriverManager.setLogStream(System.err);

        // Now run the tests
        try {
            PlotTest test = new PlotTest(args);
        }
        catch (Exception ex) {
            System.err.println("Exception caught.\n" + ex);
            ex.printStackTrace();
        }
    }
}
