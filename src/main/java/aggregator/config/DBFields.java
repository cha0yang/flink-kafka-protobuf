package aggregator.config;


import java.io.Serializable;

public class DBFields implements Serializable {

    private String field;
    private String type;

    public void setField(String field) {
        this.field = field;
    }

    public String getField() {
        return field;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getType() {
        return type;
    }

}