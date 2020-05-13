package common;

import java.io.*;

abstract public class SerializablePacket implements Serializable {
    public byte[] getBytes() throws IOException {
        ByteArrayOutputStream o = new ByteArrayOutputStream();
        ObjectOutputStream obj = new ObjectOutputStream(o);
        obj.writeObject(this);
        return o.toByteArray();
    }

    public static <T extends Serializable> T ofBytes(byte[] bytes) throws IOException, ClassNotFoundException {
        ByteArrayInputStream i = new ByteArrayInputStream(bytes);
        ObjectInputStream in = new ObjectInputStream(i);
        return (T) in.readObject();
    }
}
