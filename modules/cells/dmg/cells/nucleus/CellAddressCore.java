package dmg.cells.nucleus ;
import  java.io.Serializable ;
import  java.util.Date ;

/**
  *  
  * Is the core of the CellDomain addressing scheme.
  * The <code>CellAddressCore</code> specifies the
  * name of the cell and the name of the domain
  * the cell exists in.<br>
  * <strong>This Class is designed to be immutable. The
  * whole package relies on that fact. </strong>
  *
  * @author Patrick Fuhrmann
  * @version 0.1,02/15/1998
  *
  * 
  */
public class CellAddressCore implements Cloneable , Serializable {
   static final long serialVersionUID = 4072907109959708379L;

   private String _domain = null ;
   private String _cell   = null ;
   
   /**
     * Creates a CellAddressCore by scanning the argument string.
     * The syntax can be only one of the folling :<br>
     * <cellName> or <cellName>@<domainName>.
     * If the <domainName> is omitted, the keywork 'local' is
     * used instead.
     *
     * @param addr the cell address specification which is interpreted
     *        as described above. The specified <code>addr</code> is
     *        not checked for existance.
     * @return the immutable CellAddressCore.
     * 
     */
   public CellAddressCore( String addr ){
      int ind = addr.indexOf( '@' ) ;
      if( ind < 0 ){
          _cell   = addr ;
          _domain = "local" ;
      }else{
          _cell = addr.substring( 0 , ind ) ;
          if( ind == ( addr.length() -1 ) )
             _domain = "local" ;
          else
             _domain = addr.substring( ind+1 ) ;
      }
   }
   public CellAddressCore( String addr , String domain ){
       _cell   = addr ;
       _domain = domain ;
   }
   /*
   CellAddressCore getClone(){ 
      try { 
         return (CellAddressCore)this.clone() ; 
      }catch( CloneNotSupportedException cnse ){
         return null ;
      }
   }
   */   
   public String getCellName(){ return _cell ; }
   public String getCellDomainName(){ return _domain ; }
   public String toString(){
      return     (   _cell != null ? _cell   : "UnknownCell"   )+"@"+
                 ( _domain != null ? _domain : "UnknownDomain" ) ;
   }
   public boolean equals( Object obj ){
       if( ! ( obj instanceof CellAddressCore ) )return false ;
       CellAddressCore other = (CellAddressCore)obj ;
       return   other._domain.equals(_domain) && 
                other._cell.equals(_cell) ;
   }
   public int hashCode(){ return ( _domain+_cell ).hashCode() ; }

}
 
