package aggregator.db;

import aggregator.config.AppConfig;
import aggregator.config.DB;
import aggregator.config.DBFields;
import com.clickhouse.jdbc.ClickHouseDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class CK {
    private static final Logger LOG = LoggerFactory.getLogger(CK.class);

    private static String insertSQL;
    private static String username;
    private static String password;

    private static ClickHouseDataSource dataSource;

    public static void Init(AppConfig config) throws Exception {
        DB dc = config.getDb();

        String table = dc.getTable();
        username = dc.getUsername();
        password = dc.getPassword();

        dataSource = new ClickHouseDataSource("jdbc:ch://" + dc.getHost() + "/" + dc.getDatabase());
        try {
            Connection conn = dataSource.getConnection(dc.getUsername(), dc.getPassword());
            for (String statement : statements(config)) {
                LOG.info(statement);
                Statement stmt = conn.createStatement();
                stmt.executeQuery(statement);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        List<String> fields = new ArrayList<>();
        List<String> fieldsWithType = new ArrayList<>();
        for (DBFields f : config.getDbFields()) {
            fields.add(f.getField());
            fieldsWithType.add(f.getField() + ' ' + f.getType());
        }

        insertSQL = "insert into " + table + (!dc.getCluster().equals("") ? "_local" : "") +
                " select " + String.join(",", fields) +
                " from input('" + String.join(",", fieldsWithType) + "')";

        LOG.info(insertSQL);
    }

    private static List<String> statements(AppConfig config) {
        DB dbConfig = config.getDb();
        List<DBFields> dbFieldsList = config.getDbFields();
        String cluster = dbConfig.getCluster();
        boolean isCluster = !Objects.equals(cluster, "");

        StringBuilder createSQL = new StringBuilder("\n");

        createSQL.append(String.format("CREATE TABLE IF NOT EXISTS %s\n", dbConfig.getTable() + (isCluster ? "_local" : "")));
        createSQL.append((isCluster ? String.format("ON CLUSTER '%s'\n", cluster) : ""));
        createSQL.append("(\n");

        for (int i = 0; i < dbFieldsList.size(); i++) {
            DBFields f = dbFieldsList.get(i);
            createSQL.append(String.format("%s %s%s\n", f.getField(), f.getType(), i == dbFieldsList.size() - 1 ? "" : ","));
        }

        createSQL.append(")\n");

        createSQL.append(String.format("ENGINE %s\n", isCluster ? "ReplicatedMergeTree" : "MergeTree()"));
        createSQL.append(String.format("TTL timestamp + INTERVAL %d DAY DELETE\n", dbConfig.getTtl()));
        createSQL.append("PARTITION BY(toDate(timestamp))\n");
        createSQL.append(String.format("ORDER BY (%s)\n", String.join(",", dbConfig.getOrderBy())));
        createSQL.append("SETTINGS index_granularity = 1024");


        List<String> statements = new ArrayList<>();
        statements.add(createSQL.toString());

        if (isCluster) {
            String createDistribute = String.format("CREATE TABLE IF NOT EXISTS %s\n", dbConfig.getTable()) +
                    (String.format("ON CLUSTER '%s'\n", cluster)) +
                    String.format("ENGINE = Distributed('%s', %s, %s_local, rand())", dbConfig.getCluster(), dbConfig.getDatabase(), dbConfig.getTable());

            statements.add(createDistribute);
        }

        return statements;
    }

    public static void insert(List<Object> cols) {
        try (Connection conn = dataSource.getConnection(username, password)) {
            PreparedStatement ps = conn.prepareStatement(insertSQL);

            for (int i = 0; i < cols.size(); i++) {
                ps.setObject(i + 1, cols.get(i));
            }
            ps.addBatch();
            ps.executeBatch();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
