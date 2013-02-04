package dmg.cells.services.login ;

import java.io.Serializable;

public class Vehicle implements Serializable {
    private static final long serialVersionUID = -3400324900381551271L;
    private String _str ;
    private int    _int ;
    public Vehicle( String str , int x ){
       _str = str ;
       _int = x ;
    }
    public String getString(){ return _str ;}
    public int    getInt(){ return _int ; }

}
