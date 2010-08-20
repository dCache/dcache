package org.dcache.commons.plot.dao.jdbc;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
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

    @Override
    public void handleStatement(PreparedStatement statement, PlotRequest request) throws PlotException {
        Date startDate = request.getParameter(ParamStartDate.class);
        Date endDate = request.getParameter(ParamEndDate.class);

        try {
            if (startDate == null) {
                statement.setTimestamp(1, new Timestamp(0));
            } else {
                statement.setTimestamp(1, new Timestamp(startDate.getTime()));
            }

            if (endDate == null) {
                statement.setTimestamp(2, new Timestamp((new java.util.Date()).getTime()));
            } else {
                statement.setTimestamp(2, new Timestamp(endDate.getTime()));
            }
        } catch (SQLException ex) {
            throw new PlotException("SQL exception in preprepared statement :" + ex, ex);
        }
    }
}
