package test;

import org.postgresql.util.PSQLException;
import org.testcontainers.containers.CockroachContainer;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestCockroachConstraint
{
    @Test
    public void testConstraintPostgreSql()
            throws SQLException
    {
        testConstraint(new PostgreSQLContainer<>("postgres:13"));
    }

    @Test
    public void testConstraintCockroach()
            throws SQLException
    {
        testConstraint(new CockroachContainer("cockroachdb/cockroach:v21.1.3"));
    }

    private static void testConstraint(JdbcDatabaseContainer<?> database)
            throws SQLException
    {
        database.start();

        try (Connection connection = DriverManager.getConnection(database.getJdbcUrl(), database.getUsername(), database.getPassword())) {
            try (Statement statement = connection.createStatement()) {
                statement.execute("CREATE TABLE items (item_id INT PRIMARY KEY, name TEXT UNIQUE)");
                statement.execute("INSERT INTO items VALUES (123, 'Apple')");
                statement.execute("INSERT INTO items VALUES (456, 'Orange')");
            }

            try (Statement statement = connection.createStatement()) {
                assertThatThrownBy(() -> statement.execute("INSERT INTO items VALUES (123, 'Grape')"))
                        .isInstanceOfSatisfying(PSQLException.class, e ->
                                assertThat(e.getServerErrorMessage().getConstraint()).isIn("primary", "items_pkey"));
            }

            try (Statement statement = connection.createStatement()) {
                assertThatThrownBy(() -> statement.execute("INSERT INTO items VALUES (789, 'Apple')"))
                        .isInstanceOfSatisfying(PSQLException.class, e ->
                                assertThat(e.getServerErrorMessage().getConstraint()).isEqualTo("items_name_key"));
            }
        }
    }
}
