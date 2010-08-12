package org.dcache.commons.plot.dao.jdbc;

import java.sql.PreparedStatement;
import org.dcache.commons.plot.PlotException;
import org.dcache.commons.plot.PlotRequest;


/**
 * this class handles predefined
 * statement preparation in JDBC connectivity
 * @author timur and tao
 */
public interface DaoJdbcStatementHandler {
    void handleStatement(PreparedStatement statement, PlotRequest request)
            throws PlotException;
}
