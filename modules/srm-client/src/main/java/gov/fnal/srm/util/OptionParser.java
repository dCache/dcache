//______________________________________________________________________________
//
// This code is stolen from org.dcache.services.AbstractCell
//
//  $Id$
//  $Author$
//______________________________________________________________________________
package gov.fnal.srm.util;

import java.io.PrintWriter;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.math.BigInteger;
import java.util.HashSet;
import java.util.Set;

import org.dcache.util.Args;

public class OptionParser {

    public static Set<String> getOptions(Object o){
        Set<String> set = new HashSet<>();
        Class<?> c = o.getClass();
        while(c!=null) {
            for (Field field : c.getDeclaredFields()) {
                Option option = field.getAnnotation(Option.class);
                if (option != null) {
                    set.add(option.name());
                }
            }
            c = c.getSuperclass();
        }
        return set;
    }

    /**
     * Returns string value of option argument
     * or default.
     */
     public static String getOption(Option option,
                                    Args args)
     throws IllegalArgumentException {
         return getOption(option,args,true);
     }

     /**
      * Returns string value of option argument
      * or default or null
      */
     public static String getOption(Option option,
                                    Args args,
                                    boolean setDefault)
     throws IllegalArgumentException {
         String s;
         s = args.getOption(option.name());
         if (s != null ) {
             if (s.length()==0 && !option.required()) {
                 return "true";    // to support switch type options
             } else {
                 return s;
             }
         }
         if (option.required()) {
             throw new
                     IllegalArgumentException(option.name() +
                     " is a required argument");
         }
         if (setDefault) {
             return option.defaultValue();
         }
         return s;
     }

     /**
      * Checks if list of options contains option names that do not
      * match class field names.
      */
     public static void checkOptions(Object o, Args args) {
         for (String optionName : args.optionsAsMap().keySet()) {
             Boolean exists=false;
             Class<?> c = o.getClass();
             while(c!=null) {
                 for (Field field : c.getDeclaredFields()) {
                     Option option = field.getAnnotation(Option.class);
                     if (option==null) {
                         continue;
                     }
                     if (option.name().equals(optionName)) {
                         exists = true;
                         break;
                     }
                 }
                 c = c.getSuperclass();
             }
             if (!exists) {
                 throw new
                 IllegalArgumentException("Unknown option specified : -"+optionName);
             }
         }
     }

     /**
      * Checks if there are options set to nulls
      */

     public static void checkNullOptions(Object o, String ... names) throws IllegalArgumentException  {
         boolean haveNullOptions=false;
         StringBuilder sb = new StringBuilder();
         Class<?> c = o.getClass();
         while(c!=null) {
             for (Field field : c.getDeclaredFields()) {
                 Option option = field.getAnnotation(Option.class);
                 if (option != null) {
                     for (String s: names) {
                         if (option.name().equals(s)) {
                             try {
                                 field.setAccessible(true);
                                 Object value = field.get(o);
                                 if (value==null) {
                                     sb.append("   ").append(option.name())
                                             .append(" option must be set \n");
                                     haveNullOptions=true;
                                 }
                                 break;
                             }
                             catch (IllegalAccessException e) {
                                 throw new RuntimeException("Bug detected while processing option "+
                                         option.name(), e);
                             }
                         }
                     }
                 }
             }
             c = c.getSuperclass();
         }
         if (haveNullOptions) {
             throw new IllegalArgumentException(sb.toString());
         }
     }

     /**
      * Checks if there are options set to nulls
      */

     public static String printOptions(Object o, String ... names) throws IllegalArgumentException  {
         StringBuilder sb = new StringBuilder();
         Class<?> c = o.getClass();
         int maxlength=0;
         int nblanks=3;
         for (String s:names) {
             if(s.length()>maxlength) {
                 maxlength = s.length();
             }
         }
         int indent=maxlength+nblanks+2;
         int width=80-indent;
         while(c!=null) {
             for (Field field : c.getDeclaredFields()) {
                 Option option = field.getAnnotation(Option.class);
                 if (option != null) {
                     for (String s: names) {
                         if (option.name().equals(s)) {
                             try {
                                 field.setAccessible(true);
                                 Object value = field.get(o);
                                 for(int i=0;i<nblanks;i++) {
                                     sb.append(' ');
                                 }
                                 sb.append("-").append(option.name());
                                 if (field.getType()!= Boolean.TYPE) {
                                     sb.append('=');
                                 }
                                 else {
                                     sb.append(' ');
                                 }
                                 for (int i=option.name().length();i<maxlength;i++) {
                                     sb.append(' ');
                                 }
                                 if (field.getType()!= Boolean.TYPE) {
                                     String lines=splitStringIntoSentences(option.description(),
                                             indent,
                                             width);
                                     sb.append(lines).append('\n');
                                     for (int i=0;i<indent;i++) {
                                         sb.append(' ');
                                     }
                                     sb.append("current value is ")
                                             .append(value != null ? value : "null(not set) ")
                                             .append(" ").append(option.unit())
                                             .append("\n");
                                 }
                                 else {
                                     String lines=splitStringIntoSentences(option.description()+"(switch)",
                                             indent,
                                             width);
                                     sb.append(lines).append('\n');
                                     for (int i=0;i<indent;i++) {
                                         sb.append(' ');
                                     }
                                     sb.append("current value is ")
                                             .append(value).append("\n");
                                 }
                             }
                             catch (IllegalAccessException e) {
                                 throw new RuntimeException("Bug detected while processing option "+
                                         option.name(), e);
                             }
                         }
                     }
                 }
             }
             c = c.getSuperclass();
         }
         return sb.toString();
     }


     /**
      * Sets class fields to their default values
      */
     public static <T>  void setDefaults(T t) {
         Class<?> c = t.getClass();
         while(c!=null) {
             for (Field field : c.getDeclaredFields()) {
                 Option option = field.getAnnotation(Option.class);
                 if (option==null) {
                     continue;
                 }
                 try {
                     field.setAccessible(true);
                     String s = option.defaultValue();
                     Object value;
                     if (s != null && s.length() > 0) {
                         try {
                             value=toType(s,field.getType());
                             field.set(t,value);
                         }
                         catch (ClassCastException e) {
                             throw new
                             IllegalArgumentException("Cannot convert '"+
                                     s + "' to " + field.getType(), e);
                         }
                     }
                 }
                 catch (SecurityException | IllegalAccessException e) {
                     throw new RuntimeException("Bug detected while processing option "+
                             option.name(), e);
                 }
             }
             c = c.getSuperclass();
         }
     }

     /**
      * Sets class fields to the option values specified in args
      * or default values
      */
     public static <T>  void parseOptions(T t,
                                          Args args) {
         checkOptions(t,args);
         Class<?> c = t.getClass();
         while(c!=null) {
             for (Field field : c.getDeclaredFields()) {
                 Option option = field.getAnnotation(Option.class);
                 if (option==null) {
                     continue;
                 }
                 try {
                     field.setAccessible(true);
                     String s = getOption(option,args);
                     Object value;
                     if (s != null && s.length() > 0) {
                         try {
                             value=toType(s,field.getType());
                             field.set(t,value);
                         }
                         catch (ClassCastException e) {
                             throw new
                             IllegalArgumentException("Cannot convert '"+
                                     s + "' to " + field.getType(), e);
                         }
                     }
                 }
                 catch (SecurityException | IllegalAccessException e) {
                     throw new RuntimeException("Bug detected while processing option "+
                             option.name(), e);
                 }
             }
             c = c.getSuperclass();
         }
     }


     /**
      * Sets class fields to the option values specified in args.
      *
      */
     public static <T>  void parseSpecifiedOptions(T t,
                                                   Args args) {
         checkOptions(t,args);
         Class<?> c = t.getClass();
         while(c!=null) {
             for (Field field : c.getDeclaredFields()) {
                 Option option = field.getAnnotation(Option.class);
                 if (option==null) {
                     continue;
                 }
                 try {
                     field.setAccessible(true);
                     String s = getOption(option,args,false);
                     Object value;
                     if (s != null && s.length() > 0) {
                         try {
                             value=toType(s,field.getType());
                             field.set(t,value);
                         }
                         catch (ClassCastException e) {
                             throw new
                             IllegalArgumentException("Cannot convert '"+
                                     s + "' to " + field.getType(), e);
                         }
                     }
                 }
                 catch (SecurityException | IllegalAccessException e) {
                     throw new RuntimeException("Bug detected while processing option "+
                             option.name(), e);
                 }
             }
             c = c.getSuperclass();
         }
     }

     public static <T>  void parseOption(T t,
                                         String optionName,
                                         Args args) {
         Class<?> c = t.getClass();
         boolean exists = false;
         while(c!=null) {
             for (Field field : c.getDeclaredFields()) {
                 Option option = field.getAnnotation(Option.class);
                 if (option==null) {
                     continue;
                 }
                 if (!option.name().equals(optionName)) {
                     continue;
                 }
                 exists=true;
                 try {
                     field.setAccessible(true);
                     String s = getOption(option,args);
                     Object value;
                     if (s != null && s.length() > 0) {
                         try {
                             value=toType(s,field.getType());
                             field.set(t,value);
                         }
                         catch (ClassCastException e) {
                             throw new
                             IllegalArgumentException("Cannot convert '"+
                                     s + "' to " + field.getType(), e);
                         }
                     }
                 }
                 catch (SecurityException | IllegalAccessException e) {
                     throw new RuntimeException("Bug detected while processing option "+
                             option.name(), e);
                 }
             }
             c = c.getSuperclass();
         }
         if (!exists) {
             throw new
             IllegalArgumentException("Unknown option specified : -"+optionName);
         }
     }

     /**
      * Writes information about all options (Option annotated fields)
      * to a writer.
      */
     protected <T> void writeOptions(T t,
                                     PrintWriter out) {
         Class<?> c = t.getClass();
         while (c!=null) {
             for (Field field : c.getDeclaredFields()) {
                 Option option = field.getAnnotation(Option.class);
                 if (option==null) {
                     continue;
                 }
                 try {
                     if (option.log()) {
                         field.setAccessible(true);
                         Object value = field.get(t);
                         String description = option.description();
                         String unit = option.unit();
                         if (description.length() == 0) {
                             description = option.name();
                         }
                         out.println(description + " is " + value + " " + unit);
                     }
                 }
                 catch (SecurityException | IllegalAccessException e) {
                     throw new RuntimeException("Bug detected while processing option " +
                             option.name(), e);
                 }
             }
             c = c.getSuperclass();
         }
     }

     @SuppressWarnings("unchecked")
     static public <T> T toType(final Object object, final Class<T> type) {
         T result = null;
         if (object == null || "null".equalsIgnoreCase(object.toString()) ) {
             // Initialize primitive types:
             if (type == Boolean.TYPE) {
                 result = ((Class<T>) Boolean.class).cast(false);
             }
             else if (type == Byte.TYPE) {
                 result = ((Class<T>) Byte.class).cast(0);
             }
             else if (type == Character.TYPE) {
                 result = ((Class<T>) Character.class).cast(0);
             }
             else if (type == Double.TYPE) {
                 result = ((Class<T>) Double.class).cast(0.0);
             }
             else if (type == Float.TYPE) {
                 result = ((Class<T>) Float.class).cast(0.0);
             }
             else if (type == Integer.TYPE) {
                 result = ((Class<T>) Integer.class).cast(0);
             }
             else if (type == Long.TYPE) {
                 result = ((Class<T>) Long.class).cast(0);
             } else if (type == Short.TYPE) {
                 result = ((Class<T>) Short.class).cast(0);
             }
         }
         else {
             final String so = object.toString();
             //custom type conversions:
             if (type == BigInteger.class) {
                 result = type.cast(new BigInteger(so));
             }
             else if (type == Boolean.class || type == Boolean.TYPE) {
                 Boolean r;
                 if ("1".equals(so) || "true".equalsIgnoreCase(so) || "yes".equalsIgnoreCase(so) || "on".equalsIgnoreCase(so) || "enabled".equalsIgnoreCase(so)) {
                     r = Boolean.TRUE;
                 }
                 else if ("0".equals(object) || "false".equalsIgnoreCase(so) || "no".equalsIgnoreCase(so) || "off".equalsIgnoreCase(so) || "disabled".equalsIgnoreCase(so)) {
                     r = Boolean.FALSE;
                 }
                 else {
                     r = Boolean.valueOf(so);
                 }
                 if (type == Boolean.TYPE) {
                     result = ((Class<T>) Boolean.class).cast(r); //avoid ClassCastException through autoboxing
                 }
                 else {
                     result = type.cast(r);
                 }
             }
             else if (type == Byte.class || type == Byte.TYPE) {
                 Byte i=Byte.valueOf(so);
                 if (type == Byte.TYPE) {
                     result = ((Class<T>) Byte.class).cast(i); //avoid ClassCastException through autoboxing
                 }
                 else {
                     result = type.cast(i);
                 }
             }
             else if (type == Character.class || type == Character.TYPE) {
                 Character i = so.charAt(0);
                 if (type == Character.TYPE) {
                     result = ((Class<T>) Character.class).cast(i); //avoid ClassCastException through autoboxing
                 }
                 else {
                     result = type.cast(i);
                 }
             }
             else if (type == Double.class || type == Double.TYPE) {
                 Double i = Double.valueOf(so);
                 if (type == Double.TYPE) {
                     result = ((Class<T>) Double.class).cast(i); //avoid ClassCastException through autoboxing
                 }
                 else {
                     result = type.cast(i);
                 }
             }
             else if (type == Float.class || type == Float.TYPE) {
                 Float i = Float.valueOf(so);
                 if (type == Float.TYPE) {
                     result = ((Class<T>) Float.class).cast(i); //avoid ClassCastException through autoboxing
                 }
                 else {
                     result = type.cast(i);
                 }
             }
             else if (type == Integer.class || type == Integer.TYPE) {
                 Integer i = Integer.valueOf(so);
                 if (type == Integer.TYPE) {
                     result = ((Class<T>) Integer.class).cast(i); //avoid ClassCastException through autoboxing
                 }
                 else {
                     result = type.cast(i);
                 }
             }
             else if (type == Long.class || type == Long.TYPE) {
                 Long i = Long.valueOf(so);
                 if (type == Long.TYPE) {
                     result = ((Class<T>) Long.class).cast(i); //avoid ClassCastException through autoboxing
                 }
                 else {
                     result = type.cast(i);
                 }
             }
             else if (type == Short.class || type == Short.TYPE) {
                 Short i = Short.valueOf(so);
                 if (type == Short.TYPE) {
                     result = ((Class<T>) Short.class).cast(i); //avoid ClassCastException through autoboxing
                 }
                 else {
                     result = type.cast(i);
                 }
             }
             else {
                 try {
                     Constructor<T> constructor =
                         type.getConstructor(String.class);
                     result = constructor.newInstance(object);
                 }
                 catch (NoSuchMethodException e) {
                     //hard cast:
                         result = type.cast(object);
                 }
                 catch (SecurityException e) {
                     //hard cast:
                     result = type.cast(object);
                 }
                 catch (InstantiationException e) {
                     //hard cast:
                     result = type.cast(object);
                 }
                 catch (IllegalAccessException e) {
                     //hard cast:
                     result = type.cast(object);
                 } catch (InvocationTargetException e) {
                     //hard cast:
                     result = type.cast(object);
                 }
             }
         }
         return result;
     }

     public static String splitStringIntoSentences(String text, int start, int width){
         if (text.length()<=width) {
             return text;
         }
         StringBuilder sb = new StringBuilder();
         String[] words=text.split(" ");
         int currentLineWidth=0;
         for (String word: words) {
             currentLineWidth+=word.length()+1;
             if (currentLineWidth>width-1) {
                 sb.append('\n');
                 for (int j=0;j<start;j++) {
                     sb.append(' ');
                 }
                 currentLineWidth=0;
             }
             sb.append(word).append(' ');
         }
         return sb.toString();
     }
}
