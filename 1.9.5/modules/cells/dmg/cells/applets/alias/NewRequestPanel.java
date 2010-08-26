package dmg.cells.applets.alias ;

import java.applet.*;
import java.awt.* ;
import java.awt.event.* ;
import java.util.* ;
import java.io.* ;
import java.net.* ;
import java.lang.reflect.* ;

import dmg.util.* ;
import dmg.cells.services.* ;
import dmg.cells.nucleus.* ;

public class      NewRequestPanel 
       extends    Panel 
       implements ActionListener {
 
   private AliasDomainConnection _connection ;
   private TextField _classField    = new TextField() ;
   private TextField _argsField     = new TextField() ;
   private TextField _destField      = new TextField() ;
   private Button    _sendButton     = new Button("Send") ;
   private Button    _resetButton    = new Button("Reset" ) ;
   private Button    _createButton   = new Button("Create" ) ;
   private Label     _errorLabel     = new Label("") ;
   private RequestDesc _requestPanel = new RequestDesc() ;
   private ScrollPane  _scroll       = 
                    new ScrollPane(ScrollPane.SCROLLBARS_AS_NEEDED) ;
                    
   private CellMessage  _message = null ;
   private Object       _request = null ;
   
   public NewRequestPanel( AliasDomainConnection connection ){
      _connection = connection ;
      BorderLayout bl = new BorderLayout() ;
      bl.setHgap(10) ;
      bl.setVgap(10) ;
      setLayout(new BorderLayout());
      GridLayout gl = new GridLayout(1,0) ;
      gl.setHgap(10) ;
      gl.setVgap(10) ;

      Panel classRow = new Panel(new BorderLayout()) ;
      classRow.add( new Label("ClassName" , Label.LEFT ) , "West" );
      classRow.add( _classField , "Center" ) ;
      Panel argsRow  = new Panel(new BorderLayout()) ;
      argsRow.add( new Label( "Arguments" , Label.LEFT ) , "West" );
      argsRow.add( _argsField , "Center" ) ;
      Panel destRow  = new Panel(new BorderLayout()) ;
      Panel destWest = new Panel(new FlowLayout()) ;
      destWest.add( _createButton ) ;
      destWest.add( _sendButton ) ;
      destWest.add( _resetButton ) ;
      destWest.add( new Label( "Destination" , Label.LEFT ) ) ;
      destRow.add( destWest , "West" );
      destRow.add( _destField , "Center" ) ;
      Panel top = new Panel( new GridLayout(0,1) ) ;
      top.add( classRow ) ;
      top.add( argsRow ) ;
      top.add( destRow ) ;
      
      _createButton.addActionListener(this);
      _sendButton.addActionListener(this);
      _resetButton.addActionListener(this);
      _classField.addActionListener(this);
      _argsField.addActionListener(this);
      _destField.addActionListener(this);

      _sendButton.setEnabled(false) ;
      
      add( top , "North" ) ;
      _scroll.add( _requestPanel ) ;
      add( _scroll , "Center" ) ;
      add( _errorLabel , "South" ) ;
      
   }
   public Insets getInsets(){ return new Insets(10,10,10,10) ; }
   public void addActionListener( ActionListener al ){
   
   }
   private Class _class = null ;
   public void actionPerformed( ActionEvent event ){
      Object source = event.getSource() ;
      String command = event.getActionCommand() ;
      if( source == _classField ){

      }else if( source == _argsField ){
      
      }else if( source == _destField ){
      }else if( source == _sendButton ){
         String dest = _destField.getText() ;
         if( dest.length() == 0 ){
            _errorLabel.setText("No destination specified" ) ;
            return ;
         }
         try{
             CellMessage msg = new CellMessage( new CellPath( dest ) ,
                                                _request ) ;
             _connection.sendMessage( msg ) ;
             reset() ;
         }catch( Exception e){
             _errorLabel.setText( "Sending failed : "+e ) ;
         }
      }else if( source == _resetButton ){
         reset() ;
      }else if( source == _createButton ){
         if( createRequest( _classField.getText() , _argsField.getText() ) ){
            _createButton.setEnabled(false) ;
            _requestPanel.setRequest( _request ) ;
            _scroll.doLayout();
            _sendButton.setEnabled(true);
         }
      }
   }
   private void reset(){
     _requestPanel.reset() ;
     _sendButton.setEnabled(false);
     _createButton.setEnabled(true) ;
     _request = null ;
     _message = null ;
   }
   private boolean createRequest( String className , String argsString ){
        if( className.length() == 0 ){
           _errorLabel.setText("No class specified" ) ;
           return false ;
        }
        try{
           _class = Class.forName( className ) ;
        }catch( Exception e ){
           _errorLabel.setText("Class not found : "+className ) ;
           return false ;
        }
        Args args = new Args( argsString ) ;
        Constructor [] cons = _class.getConstructors() ;
        int argsCount = args.argc() ;
        int i = 0 ;
        Class [] argType = null ;
        for( i = 0 ; i < cons.length ; i++ ){
           argType = cons[i].getParameterTypes() ;
           if( argType.length == argsCount )break ;
        }
        if( i == cons.length ){
           _errorLabel.setText("No matching constructor found in "+className ) ;
           return false ;
        }
        Constructor c = cons[i] ;
        Object [] argObj = new Object[argType.length] ;
        for( i = 0 ; i < argsCount ; i++ ){
           if( argType[i].equals( java.lang.String.class ) ){
               argObj[i] = args.argv(i) ;
           }else if( argType[i].equals( int.class ) ){
               try{
                  argObj[i] = new Integer( args.argv(i) ) ;
               }catch( Exception e ){
                  _errorLabel.setText( "Can't convert >"+args.argv(i)+
                                        "< to "+argType[i] ) ;
               }
           }else if( argType[i].equals( float.class ) ){
               try{
                  argObj[i] = new Float( args.argv(i) ) ;
               }catch( Exception e ){
                  _errorLabel.setText( "Can't convert >"+args.argv(i)+
                                        "< to "+argType[i] ) ;
               }
           }else if( argType[i].equals( java.lang.Object.class ) ){
               try{
                  argObj[i] = args.argv(i) ;
               }catch( Exception e ){
                  _errorLabel.setText( "Can't convert >"+args.argv(i)+
                                        "< to "+argType[i] ) ;
               }
           }else if( argType[i].equals( long.class ) ){
               try{
                  argObj[i] = new Long( args.argv(i) ) ;
               }catch( Exception e ){
                  _errorLabel.setText( "Can't convert >"+args.argv(i)+
                                        "< to "+argType[i] ) ;
               }
           }else{
              _errorLabel.setText( "Unsupported argument class : "+argType[i] ) ;
              return false;
           }
        }
        
        try{
           _request = c.newInstance( argObj ) ;
        }catch( Exception ee ){
           _errorLabel.setText( "newInstance : "+ee ) ;
           return false;
        }
        _errorLabel.setText("");
        return true ;
   
   }
}
