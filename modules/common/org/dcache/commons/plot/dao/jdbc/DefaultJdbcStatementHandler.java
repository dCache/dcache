package org.dcache.commons.plot.dao.jdbc;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Date;
import org.dcache.commons.plot.ParamEndDate;
import org.dcache.commons.plot.ParamStartDate;
import org.dcache.commons.plot.PlotException;
import org.dcache.commons.plot.PlotRequest;

/**
 *
 * @author timur and tao
 */
public class DefaultJdbcStatementHandler implements DaoJdbcStatementHandler {

    public void handleStatement(PreparedStatement statement, PlotRequest request) throws PlotException {
        Date startDate = request.getParameter(ParamStartDate.class);
        Date endDate = request.getParameter(ParamEndDate.class);

        try {
            if (startDate == null) {
                statement.setDate(1, new java.sql.Date(0));
            } else {
                statement.setDate(1, new java.sql.Date(startDate.getTime()));
            }

            if (endDate == null) {
                statement.setDate(2, new java.sql.Date((new java.util.Date()).getTime()));
            } else {
                statement.setDate(2, new java.sql.Date(endDate.getTime()));
            }
        } catch (SQLException ex) {
            throw new PlotException("SQL exception in preprepared statement :" + ex, ex);
        }
    }
}
