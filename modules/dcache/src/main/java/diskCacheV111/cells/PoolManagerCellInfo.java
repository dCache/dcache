package diskCacheV111.cells ;
import  java.io.* ;
import  dmg.cells.nucleus.* ;

public class PoolManagerCellInfo
       extends CellInfo
       implements Serializable {


    private String [] _poolList = new String[0] ;

    private static final long serialVersionUID = -3079391115103078705L;

    PoolManagerCellInfo( CellInfo info ){
       super(info) ;
    }
    void setPoolList( String [] poolList ){
       _poolList = new String[poolList.length] ;
       System.arraycopy( poolList , 0 , _poolList , 0 , poolList.length);
    }
    public String [] getPoolList(){ return _poolList ; }
    public String toString(){
       StringBuilder sb = new StringBuilder() ;
       sb.append(super.toString()).append(" [") ;
        for (String pool : _poolList) {
            sb.append(pool).append(",");
        }
       sb.append("]");
       return sb.toString() ;
    }
}
