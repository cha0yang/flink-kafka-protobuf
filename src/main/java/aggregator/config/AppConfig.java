package aggregator.config;

import java.io.Serializable;
import java.util.List;

public class AppConfig implements Serializable {

    private List<Fields> fields;
    private List<DBFields> dbFields;
    private DB db;

    public void setFields(List<Fields> fields) {
        this.fields = fields;
    }

    public List<Fields> getFields() {
        return fields;
    }

    public void setDbFields(List<DBFields> db_fields) {
        this.dbFields = db_fields;
    }

    public List<DBFields> getDbFields() {
        return dbFields;
    }

    public void setDb(DB db) {
        this.db = db;
    }

    public DB getDb() {
        return db;
    }

    public Integer getWindowSeconds() {
        return windowSeconds;
    }

    public void setWindowSeconds(Integer windowSeconds) {
        this.windowSeconds = windowSeconds;
    }

    public Integer windowSeconds;

}