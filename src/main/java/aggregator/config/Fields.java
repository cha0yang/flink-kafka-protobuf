package aggregator.config;

import java.io.Serializable;

public class Fields implements Serializable {

    private String path;
    private String alias;
    private String type;

    public void setPath(String path) {
        this.path = path;
    }

    public String getPath() {
        return path;
    }

    public void setAlias(String alias) {
        this.alias = alias;
    }

    public String getAlias() {
        return alias;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getType() {
        return type;
    }

    public String getAggregateMethod() {
        return aggregateMethod;
    }

    public void setAggregateMethod(String aggregateMethod) {
        this.aggregateMethod = aggregateMethod;
    }

    private String aggregateMethod;

}