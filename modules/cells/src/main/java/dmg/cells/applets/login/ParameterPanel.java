package dmg.cells.applets.login ;

import java.awt.Label;
import java.awt.Panel;
import java.awt.TextField;
import java.util.Hashtable;
import java.util.NoSuchElementException;

public class ParameterPanel extends Panel {
    private static final long serialVersionUID = -2307385251070284472L;
    private TextField [] _valueFields;
   private Hashtable<String, TextField> _keyHash     = new Hashtable<>() ;
   public ParameterPanel( String [] parameterNames ){
      KeyValueLayout kvl = new KeyValueLayout() ;
      kvl.setFitsAllSizes(true) ;
      kvl.setHgap( 10 ) ;
      kvl.setVgap( 10 ) ;
      setLayout( kvl ) ;

      _valueFields = new TextField[parameterNames.length] ;
      String key;
      for( int i = 0 ; i < parameterNames.length ; i++ ){
         add( new Label( key = parameterNames[i]) ) ;
         add( _valueFields[i] = new TextField() ) ;
         _keyHash.put( key , _valueFields[i]  ) ;
      }
   }
   public String getValue( String key ){
      TextField x = _keyHash.get( key );
      return x == null ? null : x.getText() ;
   }
   public String getValue( int i ){
      if( i >= _valueFields.length ) {
          throw new
                  NoSuchElementException("" + i + " >= entrycount(" + _valueFields.length + ")");
      }
      return _valueFields[i].getText() ;
   }

}
