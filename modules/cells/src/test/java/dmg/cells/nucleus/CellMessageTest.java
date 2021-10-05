package dmg.cells.nucleus;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.Serializable;
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
        assertThat(decoded.getMessageObject(), is((Serializable) "payload"));
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
        assertThat(decoded.getMessageObject(), is((Serializable) "payload"));
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
}