package diskCacheV111.util;

import javax.servlet.http.HttpServletResponse;

import java.util.ArrayList;
import java.util.List;

import dmg.util.HttpException;

public class HttpByteRange
{
    private final long _lower;
    private final long _upper;

    public HttpByteRange(long lower, long upper){
        _lower = lower;
        _upper = upper;
    }

    public long getLower() {
        return _lower;
    }

    public long getUpper() {
        return _upper;
    }

    public long getSize(){
        return _upper - _lower + 1;
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
                                        throws HttpException {


        String []splitEquals=rangeString.split("=");
        if(splitEquals.length != 2 || !"bytes".equals(splitEquals[0])) {
            throw new HttpException(HttpServletResponse.SC_BAD_REQUEST,
                    "Invalid definition of ranges");
        }

        String[] csl = splitEquals[1].split(",");

        List<HttpByteRange> ret = new ArrayList<>(csl.length);
        for(String rangeSpec: csl){
            try{
                // byte-range-set may contain "optional linear whitespace"
                rangeSpec = rangeSpec.trim();
                ret.add(parseRange(rangeSpec,lower,upper));
            }catch(HttpException e){
                //RFC:  The recipient of an invalid byte-range-spec
                //      must ignore it.
                //The RFC doesn't seem to say anything about invalid suffix-range-specs
                //but I'm assuming they should be treated equally.
            }
        }

        if(ret.isEmpty()) {
            throw new HttpException(HttpServletResponse.SC_REQUESTED_RANGE_NOT_SATISFIABLE,
                    "Invalid (empty) list of valid ranges");
        }

        return ret;
    }


    /*
      * range = ( byte-range-spec | suffix-byte-range-spec )
     */
    private static HttpByteRange parseRange(String rangeSpec,
                                            long lower,
                                            long upper)
                                    throws HttpException{
        HttpByteRange ret;

        if(rangeSpec.isEmpty()) {
            throw new HttpException(HttpServletResponse.SC_BAD_REQUEST,
                    "Invalid (empty) range");
        }

        if(Character.isDigit(rangeSpec.charAt(0))){
            ret = parseRangeSpec(   rangeSpec,
                                    lower,
                                    upper);
        }else if(rangeSpec.charAt(0) == '-'){
            ret = parseSuffixRangeSpec(    rangeSpec,
                                        lower,
                                        upper);
        }else {
            throw new HttpException(HttpServletResponse.SC_BAD_REQUEST, "Invalid range");
        }

        return ret;
    }

     /*  byte-range-spec = first-byte-pos "-" [last-byte-pos] */
    private static HttpByteRange parseRangeSpec(String rangeSpec,
                                            long lower,
                                            long upper)
                                    throws HttpException {

        String[] bounds = rangeSpec.split("-");
        if(bounds.length > 2) {
            throw new HttpException(HttpServletResponse.SC_BAD_REQUEST,
                    "Invalid number of range components");
        }

        HttpByteRange ret;

        try{
            long lowerV = Long.parseLong(bounds[0]);
            long upperV = bounds.length==1 ?
                                upper :
                                Math.min(upper,Long.parseLong(bounds[1]));
            /* semantics check*/
            if(lowerV >= lower && lowerV<=upperV) {
                ret = new HttpByteRange(lowerV, upperV);
            } else {
                throw new HttpException(HttpServletResponse.SC_REQUESTED_RANGE_NOT_SATISFIABLE,
                        "Invalid range bounds");
            }
        }catch(NumberFormatException e){
            throw new HttpException(HttpServletResponse.SC_BAD_REQUEST,
                    "Invalid numeric value");
        }

        return ret;
    }

    /*  suffix-byte-range-spec = "-" suffix-length */
    private static HttpByteRange parseSuffixRangeSpec(String rangeSpec,
                                                    long lower,
                                                    long upper)
                                    throws HttpException {
        String bound = rangeSpec.substring(1);

        HttpByteRange ret;
        try{
            long suffix = Long.valueOf(bound)-1;
            if(suffix < 0) {
                throw new HttpException(HttpServletResponse.SC_BAD_REQUEST,
                        "Invalid suffix range");
            }

             long lowerV = Math.max(upper - suffix,lower);
            ret = new HttpByteRange(lowerV,upper);
        }catch(NumberFormatException e){
            throw new HttpException(HttpServletResponse.SC_BAD_REQUEST,
                    "Invalid numeric value");
        }

        return ret;
    }


}



