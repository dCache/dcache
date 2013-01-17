package org.dcache.util;

import java.sql.SQLException;

import javax.jdo.Transaction;
import javax.jdo.JDOException;

import org.springframework.orm.jdo.DefaultJdoDialect;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionException;

/**
 * Spring's JDO support is somewhat outdated: Features added in JDO
 * 2.2 or 3.0 are not supported. This JdoDialect adds some of the
 * missing features.
 */
public class Jdo3Dialect extends DefaultJdoDialect
{
    @Override
    public Object beginTransaction(Transaction transaction, TransactionDefinition definition)
        throws JDOException, SQLException, TransactionException
    {
        switch (definition.getIsolationLevel()) {
        case TransactionDefinition.ISOLATION_DEFAULT:
            break;
        case TransactionDefinition.ISOLATION_READ_UNCOMMITTED:
            transaction.setIsolationLevel("read-uncommitted");
            break;
        case TransactionDefinition.ISOLATION_READ_COMMITTED:
            transaction.setIsolationLevel("read-committed");
            break;
        case TransactionDefinition.ISOLATION_REPEATABLE_READ:
            transaction.setIsolationLevel("repeatable-read");
            break;
        case TransactionDefinition.ISOLATION_SERIALIZABLE:
            transaction.setIsolationLevel("serializable");
            break;
        }
        transaction.begin();
        return null;
    }
}
