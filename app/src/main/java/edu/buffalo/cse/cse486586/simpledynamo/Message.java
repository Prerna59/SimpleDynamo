package edu.buffalo.cse.cse486586.simpledynamo;
import java.io.Serializable;

/**
 * Created by prernasingh on 4/25/18.
 */

public class Message implements Serializable {
    private String key;
    private String value;
    private String messageType;

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public String getMessageType() {
        return messageType;
    }

    public void setMessageType(String messageType) {
        this.messageType = messageType;
    }

}
