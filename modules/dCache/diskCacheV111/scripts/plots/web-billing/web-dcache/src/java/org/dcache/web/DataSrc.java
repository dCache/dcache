/*
COPYRIGHT STATUS:
  Dec 1st 2001, Fermi National Accelerator Laboratory (FNAL) documents and
  software are sponsored by the U.S. Department of Energy under Contract No.
  DE-AC02-76CH03000. Therefore, the U.S. Government retains a  world-wide
  non-exclusive, royalty-free license to publish or reproduce these documents
  and software for U.S. Government purposes.  All documents and software
  available from this server are protected under the U.S. and Foreign
  Copyright Laws, and FNAL reserves all rights.


 Distribution of the software available from this server is free of
 charge subject to the user following the terms of the Fermitools
 Software Legal Information.

 Redistribution and/or modification of the software shall be accompanied
 by the Fermitools Software Legal Information  (including the copyright
 notice).

 The user is asked to feed back problems, benefits, and/or suggestions
 about the software to the Fermilab Software Providers.


 Neither the name of Fermilab, the  URA, nor the names of the contributors
 may be used to endorse or promote products derived from this software
 without specific prior written permission.



  DISCLAIMER OF LIABILITY (BSD):

  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
  "AS IS" AND ANY EXPRESS OR IMPLIED  WARRANTIES, INCLUDING, BUT NOT
  LIMITED TO, THE IMPLIED  WARRANTIES OF MERCHANTABILITY AND FITNESS
  FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL FERMILAB,
  OR THE URA, OR THE U.S. DEPARTMENT of ENERGY, OR CONTRIBUTORS BE LIABLE
  FOR  ANY  DIRECT, INDIRECT,  INCIDENTAL, SPECIAL, EXEMPLARY, OR
  CONSEQUENTIAL DAMAGES  (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT
  OF SUBSTITUTE  GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR
  BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY  OF
  LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
  NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT  OF THE USE OF THIS
  SOFTWARE, EVEN IF ADVISED OF THE  POSSIBILITY OF SUCH DAMAGE.


  Liabilities of the Government:

  This software is provided by URA, independent from its Prime Contract
  with the U.S. Department of Energy. URA is acting independently from
  the Government and in its own private capacity and is not acting on
  behalf of the U.S. Government, nor as its contractor nor its agent.
  Correspondingly, it is understood and agreed that the U.S. Government
  has no connection to this software and in no manner whatsoever shall
  be liable for nor assume any responsibility or obligation for any claim,
  cost, or damages arising out of or resulting from the use of the software
  available from this server.


  Export Control:

  All documents and software available from this server are subject to U.S.
  export control laws.  Anyone downloading information from this server is
  obligated to secure any necessary Government licenses before exporting
  documents or software obtained from this server.
 */
package org.dcache.web;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Hashtable;

/**
 *
 */

/**
 * @author V. Podstavkov
 *
 */
public class DataSrc
{
    protected DataSrc(Connection conn, String query)
    {
        this.conn = conn;
        this.query = query;
        colNumbers = new Hashtable<String,Integer>();
        this.titles = new ArrayList<String>();
        this.selected = new ArrayList<Integer>();

        try {
            // Prepare and execute query
            Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);

            System.err.println      ("SELECT " + query);
            rset = stmt.executeQuery("SELECT " + query);
            // Process metadata
            ResultSetMetaData rsmd = rset.getMetaData();
            numberOfColumns = rsmd.getColumnCount();
            className = new String[numberOfColumns];
            colName   = new String[numberOfColumns];
            colType   = new String[numberOfColumns];
            //
            mins      = new double[numberOfColumns];
            avgs      = new double[numberOfColumns];
            maxs      = new double[numberOfColumns];

/*
 * An example of metadata:
 * [1] classname=java.sql.Timestamp   columnname=datestamp    columntype=timestamptz
 * [2] classname=java.lang.Long       columnname=count        columntype=int8
 * [3] classname=java.math.BigDecimal columnname=fullsize     columntype=numeric
 * [4] classname=java.math.BigDecimal columnname=transfersize columntype=numeric
 */
            for (int i = 0, j = 1; i < numberOfColumns; i++, j++) {
                className[i] = rsmd.getColumnClassName(j);
                colName[i]   = rsmd.getColumnName(j);
                colType[i]   = rsmd.getColumnTypeName(j);
                if (className[0].equals("java.sql.Timestamp") && (i > 0))
                    colNumbers.put(colName[i], j+1);            // Timestamp presentation contains space inside, so we shift column number for gnuplot by one.
                else
                    colNumbers.put(colName[i], j);
                mins[i] = Double.MAX_VALUE;
                avgs[i] = 0.0;
                maxs[i] = Double.MIN_VALUE;
            }
            //
        }
        catch (SQLException ex) {
            System.err.println("Exception happened - here's what I know: ");
            ex.printStackTrace();
        }
    }

    public DataSrc(Connection conn, String tableName, String where)
    {
        this(conn, "* FROM "+tableName+" WHERE "+where);
        this.tableName = tableName;
    }

    public DataSrc(Connection conn, String tableName, Date sdate, Date edate)
    {
        // Query can be "date between '" + sdate + "' and '" + edate + "'"
        this(conn, tableName, "date >= '"+sdate+"' and date < '"+edate+"'");
    }

    public DataSrc(Connection conn, String tableName, Date sdate, Date edate, String cond)
    {
        // Query can be "date between '" + sdate + "' and '" + edate + "'"
        this(conn, tableName, "date >= '"+sdate+"' and date < '"+edate+"' "+cond);
    }

    public boolean next()
    throws SQLException
    {
        return rset.next();
    }

    public String getRow()
    throws SQLException
    {
        StringBuffer sBuffer  = new StringBuffer(128);
        for (int i = 1; i <= numberOfColumns; i++) {
            String item = rset.getString(i);
            sBuffer.append(item); sBuffer.append(" ");
        }
        return sBuffer.toString();
    }

    public String getStyle() {
        return style;
    }

    public String getTitle(int i) {
        if (i < titles.size())
            return titles.get(i);
        return titles.get(titles.size()-1);
    }

    public String getFileName() {
        return fileName;
    }

    public String getTableName() {
        if (this.tableName==null) {
            // We need to find out a table name
            int i = query.indexOf("FROM");
            if (i < 0) {
                i = query.indexOf("from");
            }
            if (i > 0) {
                this.tableName = query.substring(i).split(" ")[0];
            }
        }
        return this.tableName;
    }

    public void setStyle(String s) {
        this.style = s;
    }

    public void addTitle(String t) {
        this.titles.add(t);
    }

    public void store(String filename)
    throws SQLException, IOException
    {
        File file = new File(filename);
        PrintWriter pw = new PrintWriter(new FileOutputStream(file));
        pw.println("# "+this.query);
        pw.print("#");   for (String s: colName)   { pw.print("\t"+s); }  pw.println();
        pw.print("#");   for (String s: colType)   { pw.print("\t"+s); }  pw.println();
        pw.print("#");   for (String s: className) { pw.print("\t"+s); }  pw.println();
        int cnt = 0;
        int offset = className[0].equals("java.sql.Timestamp") ? 2 : 1;
        for (String row = null; this.next(); cnt++) {
            row = this.getRow();
            this.findMAM(row, offset);
//          row = row.replace(" ", "\t");
            pw.println(row);
        }
        numberOfRows = cnt;          // Keep the number of rows in the result
        if (cnt > 0) {
            for (int i = 0; i < avgs.length; i++) {
                avgs[i] /= cnt;
            }
        }
        pw.print("#Min:");   for (int i = 1; i < mins.length; i++) { pw.print("\t"+mins[i]); } pw.println();
        pw.print("#Avg:");   for (int i = 1; i < avgs.length; i++) { pw.print("\t"+avgs[i]); } pw.println();
        pw.print("#Max:");   for (int i = 1; i < maxs.length; i++) { pw.print("\t"+maxs[i]); } pw.println();
        pw.close(); pw = null;
        this.fileName = filename;
        this.saved = true;
    }

    private void findMAM(String row, int offset) {
        String[] tokens = row.split(" ");
        for (int i = offset, j = 1; j < mins.length; i++, j++) {
            try {
                double val = new Double(tokens[i]).doubleValue();
                if (val < mins[j]) mins[j] = val;
                if (val > maxs[j]) maxs[j] = val;
                avgs[j] += val;
            }
            catch (Exception x) {
                System.err.println("Wrong data format in row: "+row);
//                x.printStackTrace();
            }
        }
    }

    public void store()
    throws SQLException, IOException
    {
        this.store(getTableName());
    }

    public String[] getColNames() {
        return colName;
    }

    public Integer getColNumber(String name) {
        return colNumbers.get(name);
    }

    /**
     * @return Returns the avgs.
     */
    protected double getAvgs(int n) {
        if (className[0].equals("java.sql.Timestamp"))
            n--;
        return avgs[n];
    }

    /**
     * @return Returns the maxs.
     */
    protected double getMaxs(int n) {
        if (className[0].equals("java.sql.Timestamp"))
            n--;
        return maxs[n];
    }

    /**
     * @return Returns the mins.
     */
    protected double getMins(int n) {
        if (className[0].equals("java.sql.Timestamp"))
            n--;
        return mins[n];
    }

    /**
     * @return Returns the selected.
     */
    protected ArrayList<Integer> getSelected() {
        return selected;
    }

    /**
     * @param selected The selected to set.
     */
    protected void addSelected(int selected) {
        this.selected.add(selected);
    }

    /**
     * @return Returns the numberOfRows.
     */
    public int getNumberOfRows() {
        return numberOfRows;
    }

    private Connection conn;                           // Database connection
    private ResultSet rset;                            // JDBC result set
    private int numberOfColumns;                       // Number of columns in the result set
    private int numberOfRows;                          // Number of rows in the result set
    private String[] className;                        // Java class names for the columns
    private String[] colName;                          // Column names from the table
    private Hashtable<String,Integer> colNumbers;      // Column numbers in the file with the names, 'timestamp' column from the DB occupies two columns in the file
    private String[] colType;                          // Postgres type names for the columns
    private String style;
    private ArrayList<String> titles;                  // Dataset titles
    private String fileName;                           // File name with the data
    private String tableName = null;                   // Table name from the DB
    private String query;                              // SQL query
    private boolean saved = false;                     // True if dataset has been saved
    private double mins[];                             // Minimums
    private double avgs[];                             // Means
    private double maxs[];                             // Maximums
    private ArrayList<Integer> selected;               // Column numbers selected for the plot
}

