package org.example;

import java.util.stream.IntStream;

import com.tsurugidb.tsubakuro.common.Session;
import com.tsurugidb.tsubakuro.common.SessionBuilder;
import com.tsurugidb.tsubakuro.sql.SqlClient;
import com.tsurugidb.tsubakuro.sql.Transaction;
import com.tsurugidb.tsubakuro.kvs.KvsClient;
import com.tsurugidb.tsubakuro.kvs.RecordBuffer;
import com.tsurugidb.tsubakuro.kvs.TransactionHandle;
import com.tsurugidb.iceaxe.TsurugiConnector;
import com.tsurugidb.iceaxe.session.TsurugiSession;
// import com.tsurugidb.iceaxe.sql.result.TgResultMapping;
import com.tsurugidb.iceaxe.transaction.manager.TgTmSetting;
import com.tsurugidb.iceaxe.transaction.manager.TsurugiTransactionManager;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;
import java.util.ArrayList;
import java.io.IOException;
import java.net.URI;
//import java.util.stream.Stream;
//import java.util.stream.Stream.Builder;

public class App {
    public class Setting {
        private TgTmSetting tg;
        private String name;

        public Setting(TgTmSetting tg, String name) {
            this.tg = tg;
            this.name = name;
        }

        public String getName() {
            return name;
        }

        public TgTmSetting getTgTmSetting() {
            return tg;
        }
    }

    public class Table {
        private String tableName;
        private String format;
        private int rowCount;
        private int columnCount;

        public Table(String tableName, String format, int rowCount, int columnCount) {
            this.tableName = tableName;
            this.format = format;
            this.rowCount = rowCount;
            this.columnCount = columnCount;
        }

        public String getTableName() {
            return tableName;
        }

        public String getFormat() {
            return format;
        }

        public int getRowCount() {
            return rowCount;
        }

        public int getColumnCount() {
            return columnCount;
        }

        public RecordBuffer createRecordBuffer(int id) {
            RecordBuffer record = new RecordBuffer();
            if (rowCount == 3) {
                record.add("id", id);
                record.add("name", 1);
                record.add("note", 1);
            } else {
                record.add("id", id);
                record.add("name", 1);
            }
            return record;
        }
    }

    public static void drop_create(SqlClient sql, Table t) {

        String drop = String.format("DROP TABLE %s", t.getTableName());
        final String format = "create table %s " + t.getFormat();
        String create = String.format(format, t.getTableName());

        System.out.println("drop " + t.getTableName());
        try (Transaction transaction = sql.createTransaction().get()) {
            transaction.executeStatement(drop).await();
            transaction.commit().await();
            transaction.close();

        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
        System.out.println("create " + t.getTableName());
        try (Transaction transaction = sql.createTransaction().await()) {
            transaction.executeStatement(create).await();
            transaction.commit().await();
            transaction.close();
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }

    }

    public static void insert(KvsClient kvs, Table table) {
        System.out.println("insert " + table.getTableName());
        try (TransactionHandle tx = kvs.beginTransaction().await()) {
            IntStream.range(0, table.getColumnCount()).forEach(i -> {
                RecordBuffer record = table.createRecordBuffer(i);
                try {
                    kvs.put(tx, table.getTableName(), record).await();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
            kvs.commit(tx).await();
            tx.close();
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

    public static void sql_execute(Session session, SqlClient sql, KvsClient kvs) throws Exception {
        int columnCount = 1_000;
        App app = new App();
        java.util.List<Table> list = new ArrayList<>();
        list.add(app.new Table("test_table", "(id int primary key , name int , note int)", 3, columnCount));
        list.add(app.new Table("test_table_2", "(id int primary key , name int)", 2, columnCount));

        long createAndInsertTime = System.nanoTime();
        list.forEach(table -> {
            drop_create(sql, table);
            insert(kvs, table);
        });
        long createAndInsertendTime = System.nanoTime();
        System.out.println("createAndInsert " + (createAndInsertendTime - createAndInsertTime) / 1_000_000 + " nm");

    }

    private static void executeSelect(TsurugiSession session, Setting setting)
            throws IOException, InterruptedException {
        System.out.println(setting.getName());
        TsurugiTransactionManager tm = session.createTransactionManager(setting.getTgTmSetting());
        {
            String sql = "SELECT * FROM test_table cross join test_table_2";
            long start = System.nanoTime();
            tm.executeAndForEach(sql, record -> {
                // do nothing
            });
            /*
             * TgResultMapping<Integer> resultMapping = TgResultMapping.of(record ->
             * record.nextInt());
             * Builder<Integer> builder = Stream.<Integer>builder();
             * tm.executeAndForEach(sql, resultMapping, builder::add);
             * long end = System.nanoTime();
             * Stream<Integer> stream = builder.build();
             * stream.forEach(System.out::println);
             */
            long end = System.nanoTime();
            System.out.println("executeAndForEach do nothing" + (end - start) / 1_000_000 + " ms");
        }
    }

    public static void main(String[] args) throws Exception {
        URI endpoint = URI.create("tcp://localhost:12345");
        try (Session session = SessionBuilder.connect(endpoint).create();
                SqlClient sql = SqlClient.attach(session);
                KvsClient kvs = KvsClient.attach(session)) {
            sql_execute(session, sql, kvs);

        }
        TsurugiConnector connector = TsurugiConnector.of(endpoint);
        App app = new App();
        java.util.List<Setting> list = new ArrayList<>();
        list.add(app.new Setting(TgTmSetting.ofAlways(TgTxOption.ofRTX()), "RTX"));
        list.add(app.new Setting(TgTmSetting.ofAlways(TgTxOption.ofOCC()), "OCC"));
        list.add(app.new Setting(TgTmSetting.ofAlways(TgTxOption.ofLTX()), "LTX"));
        try (TsurugiSession session = connector.createSession()) {
            list.forEach(setting -> {
                try {
                    executeSelect(session, setting);
                } catch (IOException | InterruptedException e) {
                    e.printStackTrace();
                }
            });
            session.close();
        }

    }
}
