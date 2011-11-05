package dmg.cells.nucleus;

import java.util.Date;

/**
 * The InitialisableCellInfo class extends CellInfo to allow creation of a
 * CellInfo that describes some arbitrary cell. It is intended for unit
 * testing.
 */
@SuppressWarnings("serial")
public class InitialisableCellInfo extends CellInfo {

    public InitialisableCellInfo( String cellName, String cellType,
                                  String cellClass, CellVersion version,
                                  String domainName, Date creationTime,
                                  String privateInfo, String shortInfo,
                                  int eventQueueSize, int threadCount, int state) {
        super();

        // Now assign the correct values to this new CellInfo object
        setCellName( cellName);
        setCellType( cellType);
        setCellClass( cellClass);
        setCellVersion( version);
        setDomainName( domainName);
        setCreationTime( creationTime);
        setPrivateInfo( privateInfo);
        setShortInfo( shortInfo);
        setEventQueueSize( eventQueueSize);
        setThreadCount( threadCount);
        setState( state);
    }
}
