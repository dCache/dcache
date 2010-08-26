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

public class      BaseRequestPanel 
       extends    Panel 
       implements CellMessageListener    {
       
    private Font   _bigFont = 
             new Font( "SansSerif" , Font.BOLD , 24 )  ; 
   private Label  _errorLabel  ;
   private Label  _halloLabel ;
   
   public BaseRequestPanel( String name , AliasDomainConnection connection ){
      super( new BorderLayout() ) ;
      setBackground( Color.gray ) ;
      System.out.println( "Created : "+name ) ;
      _errorLabel = new Label( "O.K" ) ;
      _halloLabel = new Label( "Drive : "+name , Label.CENTER ) ;
      _halloLabel.setFont( _bigFont ) ;
      
      add( _errorLabel , "South" ) ;
//      add( _halloLabel , "North" ) ;
      
      
      Label typeLabel   = new Label( "Type -> ", Label.RIGHT) ;
      Label actionLabel = new Label( "Action -> " , Label.RIGHT ) ;
      Label rcLabel     = new Label( "RC -> " , Label.RIGHT) ;
      Label rmsgLabel   = new Label( "RMSG -> " , Label.RIGHT) ;
      

      TextField _typeText   = new TextField("type") ;
      TextField _actionText = new TextField("action") ;
      TextField _rcText     = new TextField("rc");
      TextField _rmsgText   = new TextField("rmsg");
       
      Panel topRow = new Panel( new GridLayout(0,4) ) ;
      topRow.add( typeLabel ) ;
      topRow.add( _typeText ) ;
      topRow.add( actionLabel ) ;
      topRow.add( _actionText ) ;
      Panel bottomRow = new Panel( new GridLayout(0,2) ) ;
      Panel bottomLeft = new Panel( new GridLayout(0,3) ) ;
      bottomLeft.add( rcLabel ) ;
      bottomLeft.add( _rcText ) ;
      bottomLeft.add( rmsgLabel ) ;
      bottomRow.add( bottomLeft ) ;
      bottomRow.add( _rmsgText ) ;
      
      Panel baseReq     = new Panel(new GridLayout(2,1)) ;
      baseReq.add( topRow ) ;
      baseReq.add( bottomRow ) ;
     

      add( baseReq , "North" ) ;
       
      Panel dummy = new Panel() ;
      dummy.setBackground( Color.yellow ) ; 
      add( dummy , "Center" ) ;
   }
   public Insets getInsets(){ return new Insets(20,20,20,20) ; }
   public void messageArrived( CellMessage msg ){
      _errorLabel.setText( msg.getMessageObject().toString() ) ;
   }
   private void addComponent( Container container , Component component ,
                              int gridx , int gridy ,
			      int gridwidth , int gridheight ) {
       LayoutManager lm = container.getLayout() ;
       if( ! ( lm instanceof GridBagLayout  ) ){
          System.out.println( " Not a GridBagLayout " ) ;
          return ;
       }
       GridBagConstraints gbc = new GridBagConstraints() ;
       gbc.gridx      = 0 ;
       gbc.gridy      = 0 ;
       gbc.gridwidth  = gridwidth ;
       gbc.gridheight = gridheight ;
       gbc.fill       = GridBagConstraints.BOTH ;
       gbc.anchor     = GridBagConstraints.CENTER ;
       ((GridBagLayout)lm).setConstraints( component , gbc ) ;
       container.add( component ) ;
       		  
   }
}
