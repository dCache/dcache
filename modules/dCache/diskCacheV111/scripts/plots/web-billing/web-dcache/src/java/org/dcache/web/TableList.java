package org.dcache.web;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Date;
import java.util.Vector;

import javax.sql.DataSource;


public class TableList 
{
    private Vector<TableElem> tables;
    private String name;

    public TableList() {
        tables = new Vector<TableElem>();
    }

    public void addTable(TableElem tbl) {
        tables.addElement(tbl);
    }

    public Vector<TableElem> getTables() {
        return tables;
    }

    public void setTables(Vector<TableElem> newTables) {
        tables = newTables;
    }

    public String getName() {
        return name;
    }

    public void setName(String newName) {
        name = newName;
    }

    public String toString() {
        StringBuffer buf = new StringBuffer(60);

        buf.append("TableElem List name>> " + this.getName());

        Vector tt = this.getTables();
        buf.append("\n\n**TABLES**");

        // Iterate through vectors. Append content to StringBuffer.
        for (int i = 0; i < tt.size(); i++) {
            buf.append(tt.get(i));
        }

        return buf.toString();
    }
    
    private void log(String s) 
    {
        System.out.println(new Date().toString()+": "+s);
    }
    
    public void execute(DataSource dataSource, String text) 
    {
        Vector<TableElem> tables = this.getTables(); 

        log("table list execute: "+text);
        log("table list="+this);
        log("table list size="+tables.size());
        
        // Open the database.      
        
        Connection conn = null;
        
        try {
            synchronized (dataSource) {
                conn = dataSource.getConnection();
            }
            Statement stmt = conn.createStatement();
            log("Connected to DB");
            
            for (int i = 0; i < tables.size(); i++) {
                TableElem table = tables.get(i);
                String tablename = table.getId();
                try {
                    // Check if table exists
                    stmt.executeQuery("select * from "+tablename+" limit 1");
                }
                catch (Exception ex) {
                    // and create them if doesn't
                    QueryElem createQuery = table.getCreateQuery();
                    System.err.printf("createQuery[0]>> %s%n", createQuery.get(0)); // Debug
                    stmt.executeUpdate(createQuery.get(0));
                    log("Create "+tablename);
                }
                // Now we have the table, so we can safely update the it
                QueryElem updateQuery = table.getUpdateQuery();
                int sz = updateQuery.size();
                for (int j = 0; j < sz; j++) {
                    System.err.printf("updateQuery[%d]>> %s%n", j, updateQuery.get(j));
                    stmt.executeUpdate(updateQuery.get(j));
                }
                log("Update "+tablename);
            }

        }
        catch (SQLException ex) { 
            log("Unable to update DB: "+ex.getMessage()); 
        }
        finally {
            try {
                if (conn != null) conn.close();
            }
            catch (SQLException ex) {}
        }
    }
}
