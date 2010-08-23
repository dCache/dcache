package org.dcache.commons.plot.dao.jdbc;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Date;
import org.dcache.commons.plot.ParamBinSize;
import org.dcache.commons.plot.ParamEndDate;
import org.dcache.commons.plot.ParamStartDate;
import org.dcache.commons.plot.PlotException;
import org.dcache.commons.plot.PlotRequest;
import org.dcache.commons.plot.dao.PlotDao;
import org.dcache.commons.plot.dao.TupleDateNumber;
import org.dcache.commons.plot.dao.TupleList;
import org.dcache.commons.plot.renderer.Range;

/**
 *
 * @author timur and tao
 */
public class DaoJdbc implements PlotDao {

    private String username, password, connectionURL, query, database, className;
    private DaoJdbcStatementHandler statementHandler = new DefaultJdbcStatementHandler();
    private PlotRequest plotRequest;

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

    @Override
    public TupleList getData(PlotRequest request) throws PlotException {
        plotRequest = request;
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
     * @throws PlotException
     */
    private TupleList createTupleList(ResultSet resultSet) throws PlotException {
        try {
            ResultSetMetaData metaData = resultSet.getMetaData();
            if (metaData.getColumnCount() != 2) {
                throw new PlotException("number of columns in JDBC does not match 2: "
                        + metaData.getColumnCount());
            }

            String c1 = metaData.getColumnClassName(1);
            String c2 = metaData.getColumnClassName(2);

            if (c1.compareTo(java.sql.Timestamp.class.getCanonicalName()) != 0
                    && c1.compareTo(java.sql.Timestamp.class.getCanonicalName()) != 0) {
                throw new PlotException("column 1 must be date type but is " + c1);
            }

            if (c2.compareTo(BigDecimal.class.getCanonicalName()) != 0
                    && c2.compareTo(Double.class.getCanonicalName()) != 0
                    && c2.compareTo(Integer.class.getCanonicalName()) != 0
                    && c2.compareTo(Long.class.getCanonicalName()) != 0) {
                throw new PlotException("column 2 must be number type but is " + c2);
            }

            ParamStartDate startDate = plotRequest.getParameter(ParamStartDate.class);
            ParamEndDate endDate = plotRequest.getParameter(ParamEndDate.class);
            ParamBinSize binSize = plotRequest.getParameter(ParamBinSize.class);
            if (startDate == null) {
                throw new PlotException("StartDate not specified in JDBC DAO");
            }

            if (endDate == null) {
                throw new PlotException("EndDate not specified in JDBC DAO");
            }

            if (binSize == null) {
                throw new PlotException("BinSize not specified in JDBC DAO");
            }

            Range<Date> range = new Range<Date>();
            range.setMinimum(startDate);
            range.setMaximum(endDate);

            int numBins = range.getNumBins(binSize);
            TupleList<TupleDateNumber> tupleList = new TupleList<TupleDateNumber>();

            for (int i = 0; i < numBins; i++){
                Date curDate = (Date) range.getItemAt((float)i / numBins);
                TupleDateNumber tuple = new TupleDateNumber(curDate, 0);
                tupleList.add(tuple);
            }

            while(resultSet.next()){
                Date date = new Date(resultSet.getTimestamp(1).getTime());
                BigDecimal value = resultSet.getBigDecimal(2);
                int index = (int)(range.getPosition(date) * numBins);
                BigDecimal newValue = new BigDecimal(tupleList.get(index).getYValue().doubleValue()
                        + value.doubleValue());
                tupleList.get(index).setyValue(newValue);
            }

            return tupleList;
        } catch (Exception e) {
            throw new PlotException("Exception occured in JDBC DAO: " + e, e);
        }
    }
}
