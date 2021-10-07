package dmg.cells.nucleus;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import org.junit.Test;

public class CellMessageTest {

    @Test
    public void shouldDeserializeSerializedMessage_FST() throws Exception {
        CellMessage message = new CellMessage(new CellPath("foo", "bar"), "payload");
        CellMessage encoded = message.encodeWith(SerializationHandler.Serializer.FST);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DataOutputStream outStream = new DataOutputStream(out);
        encoded.writeTo(outStream);

        DataInputStream inStream = new DataInputStream(new ByteArrayInputStream(out.toByteArray()));
        CellMessage deserialized = CellMessage.createFrom(inStream);

        CellMessage decoded = deserialized.decode();

        assertThat(decoded.getUOID(), is(message.getUOID()));
        assertThat(decoded.getLastUOID(), is(message.getUOID()));
        assertThat(decoded.getSourcePath(), is(message.getSourcePath()));
        assertThat(decoded.getDestinationPath(), is(message.getDestinationPath()));
        assertThat(decoded.getMessageObject(), is("payload"));
        assertThat(decoded.getSession(), nullValue());
        assertThat(decoded.getTtl(), is(message.getTtl()));
    }

    @Test
    public void shouldDeserializeSerializedMessage_RepackedToJos() throws Exception {
        CellMessage message = new CellMessage(new CellPath("foo", "bar"), "payload");
        CellMessage encoded = message.encodeWith(SerializationHandler.Serializer.FST);
        encoded.ensureEncodedWith(SerializationHandler.Serializer.JOS);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DataOutputStream outStream = new DataOutputStream(out);
        encoded.writeTo(outStream);

        DataInputStream inStream = new DataInputStream(new ByteArrayInputStream(out.toByteArray()));
        CellMessage deserialized = CellMessage.createFrom(inStream);

        CellMessage decoded = deserialized.decode();

        assertThat(decoded.getUOID(), is(message.getUOID()));
        assertThat(decoded.getLastUOID(), is(message.getUOID()));
        assertThat(decoded.getSourcePath(), is(message.getSourcePath()));
        assertThat(decoded.getDestinationPath(), is(message.getDestinationPath()));
        assertThat(decoded.getMessageObject(), is("payload"));
        assertThat(decoded.getSession(), nullValue());
        assertThat(decoded.getTtl(), is(message.getTtl()));
    }

    @Test(expected = IllegalStateException.class)
    public void shouldFailToSerializedUnencodedMessages() throws Exception {
        CellMessage message = new CellMessage(new CellPath("foo", "bar"), "payload");
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DataOutputStream outStream = new DataOutputStream(out);
        message.writeTo(outStream);
    }

    @Test
    public void shouldRevertDirection() {
        CellMessage message = new CellMessage(new CellPath("foo", "bar"), "payload");
        UOID umit = message.getUOID();
        CellAddressCore pathSource = new CellAddressCore("foo", "source");
        message.addSourceAddress(pathSource);

        message.revertDirection();

        assertEquals("[>foo@source]", message.getDestinationPath().toString());
        assertEquals("[empty]", message.getSourcePath().toString());
        assertEquals(umit, message.getLastUOID());
        assertNotEquals(umit, message.getUOID());
        assertTrue(message.isReply());
    }

    @Test
    public void shouldReturnFalseIfObjectIsNull() {
        CellMessage message = new CellMessage();
        assertFalse(message.equals(null));
    }

    @Test
    public void shouldReturnFalseIfObjectIsNotTheMessage() {
        CellMessage message = new CellMessage();
        Object obj = new Object();
        assertFalse(message.equals(obj));
    }

    @Test
    public void shouldReturnTrueIfObjectIsTheMessage() {
        CellMessage message = new CellMessage();
        assertTrue(message.equals(message));
    }

    @Test
    public void shouldCheckIfTwoObjectsHaveTheSameUOID() {
        CellMessage message = new CellMessage();
        CellMessage secondMessage = new CellMessage();
        CellMessage thirdMessage = new CellMessage();
        UOID umit = new UOID();
        UOID anotherUmit = new UOID();
        message.setUOID(umit);
        secondMessage.setUOID(umit);
        thirdMessage.setUOID(anotherUmit);
        assertTrue(message.equals(secondMessage));
        assertFalse(message.equals(thirdMessage));
    }

    @Test
    public void shouldReturnHashCodeOfUmid() {
        CellMessage message = new CellMessage();
        UOID umit = new UOID();
        message.setUOID(umit);
        assertEquals(message.hashCode(), umit.hashCode());
    }

    @Test(expected = IOException.class)
    public void shouldThrowAnExceptionIfReadingACellMessageThatIsNotInStreamMode()
          throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DataInputStream inStream = new DataInputStream(new ByteArrayInputStream(out.toByteArray()));
        CellMessage.createFrom(inStream);
    }
}