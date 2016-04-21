package dmg.cells.nucleus ;

/**
  *
  *
  * @author Patrick Fuhrmann
  * @version 0.1, 15 Feb 1998
  */
public class  CellEvent {
    private final Object _source;
    private final int    _type;

    public static final int EXCEPTION_EVENT = 1;
    public static final int REMOVAL_EVENT = 2;
    public static final int CELL_CREATED_EVENT = 3;
    public static final int CELL_DIED_EVENT = 4;
    public static final int CELL_EXPORTED_EVENT = 5;
    public static final int CELL_UNEXPORTED_EVENT = 6;
    public static final int CELL_ROUTE_ADDED_EVENT = 7;
    public static final int CELL_ROUTE_DELETED_EVENT = 8;
    public static final int OTHER_EVENT = 9;

    public CellEvent() {
       this(null, OTHER_EVENT);
    }

    public CellEvent(Object source , int type) {
       _source = source;
       _type = type;
    }

    public Object getSource() {
       return _source;
    }

    public int getEventType(){ return _type; }

    @Override
    public String toString(){
      String m;
      switch( _type ){
        case CELL_CREATED_EVENT : m = "CELL_CREATED_EVENT"; break;
        case CELL_DIED_EVENT : m = "CELL_DIED_EVENT"; break;
        case CELL_ROUTE_ADDED_EVENT : m = "CELL_ROUTE_ADDED_EVENT"; break;
        case CELL_ROUTE_DELETED_EVENT : m = "CELL_ROUTE_DELETED_EVENT"; break;
        default : m = "UNKNOWN";
      }
      return "Event("+m+","+_source.toString()+")";
    }
}
