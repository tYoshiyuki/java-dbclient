package dbclient;

import lombok.*;
import org.junit.Before;
import org.junit.Test;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class DbClientTest {
    private final String url = "jdbc:mysql://localhost/mysql";
    private final String username = "root";
    private final String password = "mysql";

    @Before
    public void setUp() throws SQLException {
        try (DbClient db = new DbClient(url, username, password)) {
            // テスト用のデータベース (スキーマ) を初期化する
            db.executeSql("drop database if exists dbClientTest");
            db.executeSql("create database dbClientTest");

            // テスト用のテーブルを構築する
            db.executeSql("use dbClientTest");
            db.executeSql("create table sample (\n" +
                    "    id int not null,\n" +
                    "    name nvarchar(20),\n" +
                    "    primary key(id)\n" +
                    ")\n");
            db.modify("insert into sample values(1, 'Taro Yamada')");
            db.modify("insert into sample values(2, 'Jiro Tanaka')");
            db.modify("insert into sample values(3, 'Saburo Suzuki')");

            // テスト用のストアドを作成する
            db.executeSql("create procedure sampleProc (in x int, in y int)\n" +
                    "select x + y");
        };
    }

    @Test
    public void getScalar_正常系() throws SQLException {
        // Arrange
        Long result;

        // Act
        try (DbClient db = new DbClient(url, username, password)) {
            db.executeSql("use dbClientTest");
            result = db.<Long>getScalar("select count(*) from sample");
        };

        // Assert
        assertThat(result).isEqualTo(3L);
    }

    @Test
    public void getScalar_正常系_ストアド実行() throws SQLException {
        // Arrange
        Long result;

        // Act
        try (DbClient db = new DbClient(url, username, password)) {
            db.executeSql("use dbClientTest");
            result = db.getScalar("call sampleProc(?, ?)", 1, 2);
        };

        // Assert
        assertThat(result).isEqualToComparingFieldByField(3L);
    }

    @Test
    public void getList_正常系() throws SQLException {
        // Arrange
        List<Sample> result;
        List<Sample> expect = new ArrayList<>();
        Sample s1 = Sample.builder()
                .id(1)
                .name("Taro Yamada")
                .build();
        Sample s2 = Sample.builder()
                .id(2)
                .name("Jiro Tanaka")
                .build();
        Sample s3 = Sample.builder()
                .id(3)
                .name("Saburo Suzuki")
                .build();
        expect.add(s1);
        expect.add(s2);
        expect.add(s3);

        // Act
        try (DbClient db = new DbClient(url, username, password)) {
            db.executeSql("use dbClientTest");
            result = db.getList(Sample.class,"select * from sample");
        };

        // Assert
        result.stream().forEach(r -> {
            Sample target = expect.stream().filter(e -> e.getId() == r.getId()).findFirst().get();
            assertThat(r).isEqualToComparingFieldByField(target);
        });
    }

    @Test
    public void getEntity_正常系() throws SQLException {
        // Arrange
        Sample result;
        Sample expect = Sample.builder()
                .id(2)
                .name("Jiro Tanaka")
                .build();

        // Act
        try (DbClient db = new DbClient(url, username, password)) {
            db.executeSql("use dbClientTest");
            result = db.getEntity(Sample.class,"select * from sample where id = ?", 2);
        };

        // Assert
        assertThat(result).isEqualToComparingFieldByField(expect);
    }

    @Test
    public void modify_正常系() throws SQLException {
        // Arrange
        // insert
        int ret1;
        Sample result1;
        int insertId = 4;
        String insertName = "Shiro Sato";
        Sample expect1 = Sample.builder()
                .id(insertId)
                .name(insertName)
                .build();

        // update
        int ret2;
        Sample result2;
        int updateId = 3;
        String expect2 = "Anonymous";


        // delete
        int ret3;
        Long result3;

        // Act
        try (DbClient db = new DbClient(url, username, password)) {
            db.executeSql("use dbClientTest");
            db.beginTransaction();
            // insert
            ret1 = db.modify("insert sample values (?, ?)", insertId, insertName);
            result1 = db.getEntity(Sample.class,"select * from sample where id = ?", insertId);

            // update
            ret2 = db.modify("update sample set name = ? where id = ?", expect2, updateId);
            result2 = db.getEntity(Sample.class,"select * from sample where id = ?", updateId);

            // delete
            ret3 = db.modify("delete from sample where id in (?, ?)", 1, 2);
            result3 = db.getScalar("select count(*) from sample where id in (?, ?)", 1, 2);
            db.commit();
        };

        // Assert
        // insert
        assertThat(ret1).isEqualTo(1);
        assertThat(result1).isEqualToComparingFieldByField(expect1);

        // update
        assertThat(ret2).isEqualTo(1);
        assertThat(result2.getName()).isEqualTo(expect2);

        // delete
        assertThat(ret3).isEqualTo(2);
        assertThat(result3).isEqualTo(0L);
    }

    @Test
    public void beginTransactionAndRollback_正常系() throws SQLException {
        // Arrange
        int ret;
        Sample result;
        int updateId = 3;
        String expect = "Saburo Suzuki";

        // Act
        try (DbClient db = new DbClient(url, username, password)) {
            db.executeSql("use dbClientTest");
            db.beginTransaction();
            ret = db.modify("update sample set name = ? where id = ?", "Anonymous", updateId);
            db.rollback();

            db.executeSql("use dbClientTest");
            result = db.getEntity(Sample.class,"select * from sample where id = ?", updateId);
        };

        // Assert
        assertThat(ret).isEqualTo(1);
        assertThat(result.getName()).isEqualTo(expect);
    }

    @Builder
    @Getter
    @Setter
    @AllArgsConstructor
    @NoArgsConstructor
    public static class Sample {
        private Integer id;

        private String name;
    }

}