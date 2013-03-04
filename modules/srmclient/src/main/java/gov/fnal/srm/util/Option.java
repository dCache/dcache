//______________________________________________________________________________
//
// this class is stolen from org.dcache.services
//
// $Id$
// $Author$
//______________________________________________________________________________


package gov.fnal.srm.util;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation used for cell options.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface Option
{
    String  name();
    String  description()  default "";
    String  defaultValue() default "null";
    String  unit()         default "";
    boolean required()     default false;
    boolean log()          default true;   // this field will get the option printed
    boolean save()         default false;  // this field will get this option saved to XML
}
