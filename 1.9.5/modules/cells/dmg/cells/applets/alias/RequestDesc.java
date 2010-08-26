package dmg.cells.applets.alias ;

import java.lang.reflect.* ;
import java.applet.*;
import java.awt.* ;
import java.awt.event.* ;
import java.util.* ;
import java.io.* ;
import java.net.* ;

import dmg.util.* ;
import dmg.cells.services.* ;
import dmg.cells.nucleus.* ;


public class   RequestDesc 
       extends Panel            {

     private ReqDescPanel _request = null ;
     public RequestDesc(){
         super( new BorderLayout() ) ;
     
     }
     public Insets getInsets(){ return new Insets(15,15,15,15) ; }
     public void setRequest( Object o ){
         if( _request != null )removeAll() ;
         _request = new ReqDescPanel( o.getClass() , o ) ;
         add( _request , "North" ) ;
         doLayout() ;
//         System.out.println("Layout done" );  
         validateTree() ;  
     }
     public void reset(){ 
         if( _request != null )removeAll() ;
         _request = null ;
         doLayout() ;
         validateTree() ;  
     }
}
