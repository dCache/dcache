package org.dcache.commons.util;
import org.junit.Test;
import static org.junit.Assert.assertArrayEquals;

/**
 *
 * @author timur
 */
public class StringsTests {

    String testNullString = null;
    String[] testNullStringExpectedSplit=new String[0];

    String testString1 = "";
    String[] testString1ExpectedSplit=new String[0];

    String testString2 = "  arg1 ";
    String[] testString2ExpectedSplit=new String[] {
        "arg1"};
    String testString3 = " \"arg1\" ";
    String[] testString3ExpectedSplit=new String[] {
        "arg1"};
    String testString4 = " 'arg1' ";
    String[] testString4ExpectedSplit=new String[] {
        "arg1"};
    String testString5 =
            " \"arg1 arg2\" ";
    String[] testString5ExpectedSplit=new String[] {
        "arg1 arg2"};
    String testString6 =
            " 'arg1 arg2' ";
    String[] testString6ExpectedSplit=new String[] {
        "arg1 arg2"};
    String testString7 =
            " arg1 arg2 ";
    String[] testString7ExpectedSplit=new String[] {
        "arg1", "arg2"};
    String testString8 =
            " arg1 'arg2' ";
    String[] testString8ExpectedSplit=new String[] {
        "arg1", "arg2"};
    String testString9 =
            " arg1 'arg2' \"arg3\"";
    String[] testString9ExpectedSplit=new String[] {
        "arg1", "arg2", "arg3"};

    @Test
    public void testEmptyStringSplit() {
        String[] splitString  = Strings.splitArgumentString(testNullString);
        assertArrayEquals(splitString, testNullStringExpectedSplit);
    }

    @Test
    public void testString1Split() {
        String[] splitString  = Strings.splitArgumentString(testString1);
        assertArrayEquals(splitString, testString1ExpectedSplit);
    }

    @Test
    public void testString2Split() {
        String[] splitString  = Strings.splitArgumentString(testString2);
        assertArrayEquals(splitString, testString2ExpectedSplit);
    }

    @Test
    public void testString3Split() {
        String[] splitString  = Strings.splitArgumentString(testString3);
        assertArrayEquals(splitString, testString3ExpectedSplit);
    }

    @Test
    public void testString4Split() {
        String[] splitString  = Strings.splitArgumentString(testString4);
        assertArrayEquals(splitString, testString4ExpectedSplit);
    }

    @Test
    public void testString5Split() {
        String[] splitString  = Strings.splitArgumentString(testString5);
        assertArrayEquals(splitString, testString5ExpectedSplit);
    }

    @Test
    public void testString6Split() {
        String[] splitString  = Strings.splitArgumentString(testString6);
        assertArrayEquals(splitString, testString6ExpectedSplit);
    }

    @Test
    public void testString7Split() {
        String[] splitString  = Strings.splitArgumentString(testString7);
        assertArrayEquals(splitString, testString7ExpectedSplit);
    }

    @Test
    public void testString8Split() {
        String[] splitString  = Strings.splitArgumentString(testString8);
        assertArrayEquals(splitString, testString8ExpectedSplit);
    }

    @Test
    public void testString9Split() {
        String[] splitString  = Strings.splitArgumentString(testString9);
        assertArrayEquals(splitString, testString9ExpectedSplit);
    }

}
