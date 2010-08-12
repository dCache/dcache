package org.dcache.commons.plot.dao.jdbc;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import org.dcache.commons.plot.PlotException;
import org.dcache.commons.plot.PlotRequest;
import org.dcache.commons.plot.dao.PlotDao;
import org.dcache.commons.plot.dao.TupleDateNumber;
import org.dcache.commons.plot.dao.TupleList;

/**
 *
 * @author timur and tao
 */
public class DaoJdbc implements PlotDao {

    private String username, password, connectionURL, query, database, className;
    private DaoJdbcStatementHandler statementHandler = new DefaultJdbcStatementHandler();

    public DaoJdbc() {
    }

    public DaoJdbc(DaoJdbc peer) {
        username = peer.username;
        password = peer.password;
        connectionURL = peer.connectionURL;
        query = peer.query;
        className = peer.className;
        database = peer.database;
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public DaoJdbcStatementHandler getStatementHandler() {
        return statementHandler;
    }

    public void setStatementHandler(DaoJdbcStatementHandler statementHandler) {
        this.statementHandler = statementHandler;
    }

    public String getClassName() {
        return className;
    }

    public void setClassName(String className) {
        this.className = className;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public TupleList getData(PlotRequest request) throws PlotException {
        try {
            Class.forName(className);
        } catch (ClassNotFoundException ex) {
            throw new PlotException("class forName failed: " + ex, ex);
        }
        try {
            Connection connection = DriverManager.getConnection(connectionURL, username, password);
            PreparedStatement statement = connection.prepareStatement(query);
            statementHandler.handleStatement(statement, request);
            ResultSet resultSet = statement.executeQuery();

            TupleList results = createTupleList(resultSet);
            connection.close();
            return results;
        } catch (SQLException ex) {
            throw new PlotException("SQL exception: " + ex, ex);
        }
    }

    public String getClassNameJDBC() {
        return className;
    }

    public void setClassNameJDBC(String className) {
        this.className = className;
    }

    public String getConnectionURL() {
        return connectionURL;
    }

    public void setConnectionURL(String connectionURL) {
        this.connectionURL = connectionURL;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getQuery() {
        return query;
    }

    public void setQuery(String query) {
        this.query = query;
    }

    public String getUserName() {
        return username;
    }

    public void setUserName(String userName) {
        this.username = userName;
    }

    /**
     * create a tuple list from a result set
     * @param resultSet
     * @return
     * @throws SQLException
     */
    private static TupleList createTupleList(ResultSet resultSet) throws SQLException {
        ResultSetMetaData metaData = resultSet.getMetaData();
        int numColumns = metaData.getColumnCount();

        String[] columnClassNames = new String[numColumns];
        for (int i = 0; i < numColumns; i++) {
            columnClassNames[i] = metaData.getColumnClassName(i + 1);
        }

        //if the first column is date, and the rest is Number,
        //create a date vs number tuple list
        if (numColumns > 1
                && columnClassNames[0].compareTo(java.sql.Date.class.getCanonicalName())
                == 0) {
            boolean numbers = true;
            for (int col = 1; col < numColumns; col++) {
                if (columnClassNames[col].compareTo(BigDecimal.class.getCanonicalName()) != 0
                        && columnClassNames[col].compareTo(Long.class.getCanonicalName()) != 0) {
                    numbers = false;
                    break;
                }
            }
            if (numbers) {
                TupleList<TupleDateNumber> list = new TupleList<TupleDateNumber>();
                while (resultSet.next()) {
                    List<Number> yValues = new ArrayList<Number>();
                    Date xValue = new Date(
                            resultSet.getDate(1).getTime());
                    for (int col = 2; col <= numColumns; col++) {
                        yValues.add(new BigDecimal(((Number) resultSet.getObject(col)).doubleValue()));
                    }
                    list.add(new TupleDateNumber(xValue, yValues));
                }
                return list;
            }
        }

        //general case
        while (resultSet.next()) {
            Object[] curRow = new Object[numColumns];
            for (int col = 0; col < numColumns; col++) {
                curRow[col] = resultSet.getObject(col + 1);
            }
        }
        return null;
    }
}
