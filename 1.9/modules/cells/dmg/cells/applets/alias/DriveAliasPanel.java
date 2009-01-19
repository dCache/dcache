package dmg.cells.applets.alias ;

import java.applet.*;
import java.awt.* ;
import java.awt.event.* ;
import java.util.* ;
import java.io.* ;
import java.net.* ;

import dmg.util.* ;
import dmg.cells.services.* ;
import dmg.cells.nucleus.* ;

public class      DriveAliasPanel 
       extends    Panel 
       implements CellMessageListener    {
       
    private Font   _bigFont = 
             new Font( "SansSerif" , Font.BOLD , 24 )  ; 
   private Label  _errorLabel  ;
   private Label  _halloLabel ;
   
   public DriveAliasPanel( String name , AliasDomainConnection connection ){
      super( new BorderLayout() ) ;
      setBackground( Color.green ) ;
      System.out.println( "Created : "+name ) ;
      _errorLabel = new Label( "O.K" ) ;
      _halloLabel = new Label( "Drive : "+name , Label.CENTER ) ;
      _halloLabel.setFont( _bigFont ) ;
      
      add( _errorLabel , "South" ) ;
      add( _halloLabel , "North" ) ;
      
      Panel dummy = new Panel() ;
      dummy.setBackground( Color.yellow ) ; 
      add( dummy , "Center" ) ;
   }
   public Insets getInsets(){ return new Insets(20,20,20,20) ; }
   public void messageArrived( CellMessage msg ){
      _errorLabel.setText( msg.getMessageObject().toString() ) ;
   }
}
