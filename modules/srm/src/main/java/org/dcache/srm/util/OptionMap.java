// $Id$
// $Log: not supported by cvs2svn $
// Revision 1.11  2006/01/26 04:40:59  timur

package org.dcache.srm.util;
import java.util.HashMap;
import java.util.Map;


// Flexible typed argument parsing facility
// Supported types String, Integer . Any other type can be added as well
// Each instance of the option map will filter only attributes that are constructable by Factory and have "--<attr name>=<attr value" 
// syntax
public class OptionMap<Type> {

    static class NonComplientArgument extends Exception {
        private static final long serialVersionUID = -8962392593241362212L;
    }

    private Map<String, Type> _optionMap = new HashMap<>();

    // Used to construct template type from the string values available as input argument
    public interface Factory<T> {
       public T make(String name,String value) throws NonComplientArgument;
    }

    // ConstrainedFactoryImpl will build only attributed that have been known to this factory instance
    // ConstrainedFactoryImpl will skip all others
    public interface ConstrainedFactory<T> {
       public T make(String value) throws NonComplientArgument;
    }

    static abstract class ConstrainedFactoryImpl<Type> implements Factory<Type>,ConstrainedFactory<Type>{
       ConstrainedFactoryImpl(String [] mustBeIntAttributes){ this.mustBeIntAttributes = mustBeIntAttributes; }

       @Override
       public Type make(String name,String value) throws NonComplientArgument{
           for (String mustBeIntAttribute : mustBeIntAttributes) {
               if (mustBeIntAttribute.equals(name)) {
                   return make(value);
               }
           }
           throw new NonComplientArgument(); 
       }

       private String [] mustBeIntAttributes;
    }

    public static class StringFactory implements Factory<String> {
          @Override
          public String make(String name,String value){ return value; }
    }

    public static class IntFactory extends ConstrainedFactoryImpl<Integer> {
          public IntFactory(String [] mustBeIntAttributes){ super(mustBeIntAttributes);  }

          @Override
          public Integer make(String value) throws NonComplientArgument{ 
                return Integer.valueOf(value);
          }
    }

    public OptionMap(Factory<Type> f, String [] argList) 
    {

        for (String arg : argList) {
            try {
                String name = parseName(arg);
                String value = parseValue(arg);

                Type t = f.make(name, value);
                set(name, t);
            } catch (NonComplientArgument ex) {
            }

        }
    }

    public OptionMap(){
    }

    public Type get(String argValue) 
    {
        return _optionMap.get(argValue);
    }

    public void set(String argName,Type argValue){
         _optionMap.put(argName,argValue);
    }

    private String parseName(String arg) throws NonComplientArgument{
       if ( arg.indexOf("--") == 0 ){
         int pos = arg.indexOf("=");
         if ( pos > 0 && pos < arg.length() ){
            return arg.substring(2,pos);
         }
       }  
       throw new NonComplientArgument(); 
    }
    private String parseValue(String arg) throws NonComplientArgument {
        if ( arg.indexOf("--") == 0 ){
         int pos = arg.indexOf("=");
         if ( pos > 0 && pos < arg.length() ){
            return arg.substring(pos+1,arg.length());
         }
       }
       throw new NonComplientArgument();

    }

    static public void main(String [] argList){

        String [] intAttrs = { "attr1", "attr2", "attr3" ,"otherarg" };
        String [] args = { "--attr1=1","--attr2=2", "--attr3=3" , "--otherarg=aaa" };

           OptionMap<Integer> intMap = new OptionMap<>(new OptionMap.IntFactory(intAttrs),
                                                            args );
           System.out.println(intMap.get("attr1") + intMap.get("attr3") + intMap.get("attr2") );

           OptionMap<String> sMap = new OptionMap<>(new OptionMap.StringFactory(),
                                                            args ); 

           System.out.println(sMap.get("attr1") + sMap.get("attr3") + sMap.get("attr2") + sMap.get("otherarg") );
    }
}
