/*
 * Copyright 1999-2006 University of Chicago
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.dcache.ftp.client;

import java.util.List;
import java.util.ArrayList;
import java.util.StringTokenizer;

/**
 * Represents features supported by server (as returned by FEAT command).
 * Use the static members of this class to refer to well known feature names.
 * Example: check if the server supports PARALLEL feature:
 * <pre>
 * FeatureList fl = new FeatureList(client.getFeatureList());
 * if (fl.contains(FeatureList.PARALLEL)) {
 * ...
 * }
 * </pre>
 **/

public class FeatureList
{

    /**
     * RFC 2389 specified the following syntax for FEAT responce
     * <pre>
     * feat-response   = error-response / no-features / feature-listing
     *  no-features     = "211" SP *TCHAR CRLF
     *  feature-listing = "211-" *TCHAR CRLF
     *                    1*( SP feature CRLF )
     *                    "211 End" CRLF
     *  feature         = feature-label [ SP feature-parms ]
     *  feature-label   = 1*VCHAR
     *  feature-parms   = 1*TCHAR
     * </pre>
     * Feature class represence each individual feature and contain two fields
     * required label and optional parms
     */
    public static final class Feature
    {

        private final String label;

        private final String parms;

        private Feature(String label)
        {

            if (label == null) {
                throw new NullPointerException("label is null");
            }

            this.label = label;
            this.parms = null;

        }

        private Feature(String label, String parms)
        {

            if (label == null) {
                throw new NullPointerException("label is null");
            }

            this.label = label;
            this.parms = parms;

        }

        /**
         * @return the name
         */
        public String getLabel()
        {

            return label;

        }

        /**
         * @return the qualifiers, null if no qualifiers
         */
        public String getParms()
        {

            return parms;

        }

        @Override
        public boolean equals(Object obj)
        {

            if (!(obj instanceof Feature)) {
                return false;
            }

            Feature aFeature = (Feature) obj;

            if (!label.equals(aFeature.label)) {
                return false;
            }

            if (parms == null) {
                return aFeature.parms == null;
            }

            return parms.equals(aFeature.parms);
        }
    }

    // well known labels
    public static final String SIZE = "SIZE";
    public static final String MDTM = "MDTM";
    public static final String PARALLEL = "PARALLEL";
    public static final String ESTO = "ESTO";
    public static final String ERET = "ERET";
    public static final String SBUF = "SBUF";
    public static final String ABUF = "ABUF";
    public static final String DCAU = "DCAU";
    public static final String PIPE = "PIPE";
    public static final String MODEX = "MODEX";
    public static final String GETPUT = "GETPUT";
    public static final String CKSUM = "CKSUM";

    protected final List<Feature> features = new ArrayList();

    public FeatureList(String featReplyMsg)
    {

        StringTokenizer responseTokenizer
                = new StringTokenizer(featReplyMsg,
                                      System.getProperty("line.separator"));

        // ignore the first part of the message
        if (responseTokenizer.hasMoreElements()) {
            responseTokenizer.nextToken();
        }

        while (responseTokenizer.hasMoreElements()) {

            String line = (String) responseTokenizer.nextElement();
            line = line.trim().toUpperCase();
            if (line.startsWith("211 END")) {
                break;
            }
            String[] splitFeature = line.split(" ");

            if (splitFeature.length == 2) {
                features.add(new Feature(splitFeature[0], splitFeature[1]));
            } else {
                features.add(new Feature(line));
            }

        }
    }

    public boolean contains(String label)
    {

        if (label == null) {
            throw new IllegalArgumentException("label is null");
        }
        //split argument using white space.
        String[] tokens = label.split(" ", 2);
        if (tokens.length < 1) {
            throw new IllegalArgumentException("label is empty");
        }
        label = tokens[0].toUpperCase();
        String parms = null;
        if (tokens.length > 1) {
            parms = tokens[1];
        }

        for (Feature feature : features) {
            if (feature.getLabel().equals(label)) {
                if (parms == null) {
                    //if parms are not specified as a part of the argument
                    // string, we compare labels only
                    return true;
                } else {
                    //if parms are specified, we compare them as well
                    Feature argFeature = new Feature(label, parms);
                    if (argFeature.equals(feature)) {
                        return true;
                    }
                }
            }
        }

        return false;

    }

    /**
     * Get all features that have label equal to the argument
     * Note that  RFC 2389 does not require a feature with a
     * given label to appear only once
     *
     * @param label
     * @return List of found features with given label in the same order
     * as they were given to us by the server
     */
    public List<Feature> getFeature(String label)
    {

        if (label == null) {
            throw new IllegalArgumentException("feature label is null");
        }

        label = label.toUpperCase();
        List<Feature> foundFeatures = new ArrayList();
        for (Feature feature : features) {
            if (feature.getLabel().equals(label)) {
                foundFeatures.add(feature);
            }
        }

        return foundFeatures;

    }
}
