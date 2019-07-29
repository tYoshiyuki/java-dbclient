package dbclient;

import org.apache.commons.dbutils.DbUtils;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.ResultSetHandler;
import org.apache.commons.dbutils.handlers.BeanHandler;
import org.apache.commons.dbutils.handlers.BeanListHandler;
import org.apache.commons.dbutils.handlers.ScalarHandler;

import java.sql.*;
import java.util.List;

/**
 * DBクライアント
 * DbUtilsを用いたCRUD処理を行います
 */
public class DbClient implements AutoCloseable {
    private Connection con;
    private QueryRunner qr;
    private String url;
    private String username;
    private String password;

    public DbClient(String url, String username, String password) throws SQLException {
        this.url = url;
        this.username = username;
        this.password = password;
        qr = new QueryRunner();
        getConnection();
    }

    private Connection getConnection() throws SQLException {
        if (con == null || con.isClosed()) {
            con = DriverManager.getConnection(url, username, password);
        }
        return con;
    }

    /**
     * SQLクエリを実行し、結果として単一値を取得します
     *
     * @param sql        SQL文字列
     * @param parameters SQLパラメータ
     * @param <T>        戻り値の型
     * @return
     * @throws SQLException
     */
    public <T> T getScalar(String sql, Object... parameters) throws SQLException {
        ScalarHandler<T> resultSetHandler = new ScalarHandler<T>(1);
        return qr.query(getConnection(), sql, resultSetHandler, parameters);
    }

    /**
     * SQLクエリを実行し、結果としてエンティティを取得します
     *
     * @param targetClass 戻り値のクラス情報
     * @param sql         SQL文字列
     * @param parameters  SQLパラメータ
     * @param <T>         戻り値の型
     * @return
     * @throws SQLException
     */
    public <T> T getEntity(Class<T> targetClass, String sql, Object... parameters) throws SQLException {
        BeanHandler<T> beanHandler = new BeanHandler<T>(targetClass);
        return qr.query(getConnection(), sql, beanHandler, parameters);
    }

    /**
     * SQLクエリを実行し、結果としてエンティティのリストを取得します
     *
     * @param targetClass 戻り値のクラス情報
     * @param sql         SQL文字列
     * @param parameters  SQLパラメータ
     * @param <T>         戻り値の型
     * @return
     * @throws SQLException
     */
    public <T> List<T> getList(Class<T> targetClass, String sql, Object... parameters) throws SQLException {
        ResultSetHandler<List<T>> resultSetHandlers = new BeanListHandler<>(targetClass);
        return qr.query(getConnection(), sql, resultSetHandlers, parameters);
    }

    /**
     * SQL文字列 (DDlを含む) を実行し、結果として単一値を取得します
     *
     * @param sql        SQL文字列
     * @param parameters SQLパラメータ
     * @return
     * @throws SQLException
     */
    public void executeSql(String sql, Object... parameters) throws SQLException {
        ScalarHandler<Integer> resultSetHandler = new ScalarHandler<>(1);
        qr.execute(getConnection(), sql, resultSetHandler, parameters);
    }

    /**
     * insert / update / delete を実行します
     *
     * @param sql        SQL文字列
     * @param parameters SQLパラメータ
     * @return 変更のあった件数
     * @throws SQLException
     */
    public int modify(String sql, Object... parameters) throws SQLException {
        return qr.update(con, sql, parameters);
    }

    /**
     * トランザクションを明示的に開始します
     *
     * @throws SQLException
     */
    public void beginTransaction() throws SQLException {
        con.setAutoCommit(false);
    }

    /**
     * トランザクションをコミットします
     * 実行中のコネクションはクローズします
     */
    public void commit() {
        DbUtils.commitAndCloseQuietly(con);
    }

    /**
     * トランザクションをロールバックします
     * 実行中のコネクションはクローズします
     */
    public void rollback() {
        DbUtils.rollbackAndCloseQuietly(con);
    }

    @Override
    public void close() throws SQLException {
        if (con != null && !con.isClosed()) {
            DbUtils.closeQuietly(con);
        }
    }
}