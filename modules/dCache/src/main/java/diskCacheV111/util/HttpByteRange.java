package diskCacheV111.util;

import java.text.ParseException;
import java.util.List;
import java.util.ArrayList;

import org.dcache.util.Interval;

public class HttpByteRange extends Interval{

    public HttpByteRange(long lower, long upper){
        super(lower,upper);
    }

    public long getSize(){
        return getUpper()-getLower() + 1;
    }


    /*
     *  ranges-specifier = byte-ranges-specifier
     *  byte-ranges-specifier = bytes-unit "=" byte-range-set
     *  byte-range-set  = 1#( byte-range-spec | suffix-byte-range-spec )
     *  byte-range-spec = first-byte-pos "-" [last-byte-pos]
     *  first-byte-pos  = 1*DIGIT
     *  last-byte-pos   = 1*DIGIT
     *  suffix-byte-range-spec = "-" suffix-length
     *  suffix-length = 1*DIGIT
     */

    public static List<HttpByteRange> parseRanges(String rangeString,
                                                long lower,
                                                long upper)
                                        throws ParseException{


        String []splitEquals=rangeString.split("=");
        if(splitEquals.length != 2 || !"bytes".equals(splitEquals[0]))
            throw new ParseException("Invalid definition of ranges",-1);

        String[] csl = splitEquals[1].split(",");

        List<HttpByteRange> ret = new ArrayList(csl.length);
        for(String rangeSpec: csl){
            try{
                // byte-range-set may contain "optional linear whitespace"
                rangeSpec = rangeSpec.trim();
                ret.add(parseRange(rangeSpec,lower,upper));
            }catch(ParseException e){
                //RFC:  The recipient of an invalid byte-range-spec
                //      must ignore it.
                //The RFC doesn't seem to say anything about invalid suffix-range-specs
                //but I'm assuming they should be treated equally.
            }
        }

        if(ret.size() == 0)
            throw new ParseException("Invalid (empty) list of valid ranges",-1);

        return ret;
    }


    /*
      * range = ( byte-range-spec | suffix-byte-range-spec )
     */
    private static HttpByteRange parseRange(String rangeSpec,
                                            long lower,
                                            long upper)
                                    throws ParseException{
        HttpByteRange ret;

        if(rangeSpec.isEmpty())
            throw new ParseException("Invalid (empty) range",-1);

        if(Character.isDigit(rangeSpec.charAt(0))){
            ret = parseRangeSpec(   rangeSpec,
                                    lower,
                                    upper);
        }else if(rangeSpec.charAt(0) == '-'){
            ret = parseSuffixRangeSpec(    rangeSpec,
                                        lower,
                                        upper);
        }else
            throw new ParseException("Invalid range",-1);

        return ret;
    }

     /*  byte-range-spec = first-byte-pos "-" [last-byte-pos] */
    private static HttpByteRange parseRangeSpec(String rangeSpec,
                                            long lower,
                                            long upper)
                                    throws ParseException{

        String[] bounds = rangeSpec.split("-");
        if(bounds.length > 2)
            throw new ParseException("Invalid number of range components" ,-1);

        HttpByteRange ret;

        try{
            long lowerV = Long.valueOf(bounds[0]);
            long upperV = bounds.length==1 ?
                                upper :
                                Math.min(upper,Long.valueOf(bounds[1]));
            /* semantics check*/
            if(lowerV >= lower && lowerV<=upperV)
                ret = new HttpByteRange(lowerV, upperV);
            else
                throw new ParseException("Invalid range bounds", -1);
        }catch(NumberFormatException e){
            throw new ParseException("Invalid numeric value", -1);
        }

        return ret;
    }

    /*  suffix-byte-range-spec = "-" suffix-length */
    private static HttpByteRange parseSuffixRangeSpec(String rangeSpec,
                                                    long lower,
                                                    long upper)
                                    throws ParseException{
        String bound = rangeSpec.substring(1);

        HttpByteRange ret;
        try{
            long suffix = Long.valueOf(bound)-1;
            if(suffix < 0)
                throw new ParseException("Invalid suffix range", -1);

             long lowerV = Math.max(upper - suffix,lower);
            ret = new HttpByteRange(lowerV,upper);
        }catch(NumberFormatException e){
            throw new ParseException("Invalid numeric value", -1);
        }

        return ret;
    }


}



