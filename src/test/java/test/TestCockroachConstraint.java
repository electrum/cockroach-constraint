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
        testConstraint(new PostgreSQLContainer<>("postgres:13"), false);
    }

    @Test
    public void testConstraintCockroach()
            throws SQLException
    {
        testConstraint(new CockroachContainer("cockroachdb/cockroach:v21.1.3"), true);
    }

    private static void testConstraint(JdbcDatabaseContainer<?> database, boolean cockroach)
            throws SQLException
    {
        database.start();

        try (Connection connection = DriverManager.getConnection(database.getJdbcUrl(), database.getUsername(), database.getPassword())) {
            try (Statement statement = connection.createStatement()) {
                statement.execute("CREATE TABLE items (item_id INT PRIMARY KEY, name TEXT UNIQUE, a INT, b INT, UNIQUE (a, b))");
                statement.execute("INSERT INTO items VALUES (123, 'Apple', 1, 2)");
                statement.execute("INSERT INTO items VALUES (456, 'Orange', 3, 4)");
            }

            try (Statement statement = connection.createStatement()) {
                assertThatThrownBy(() -> statement.execute("INSERT INTO items VALUES (123, 'Grape')"))
                        .isInstanceOfSatisfying(PSQLException.class, e -> {
                            assertThat(e.getServerErrorMessage()).isNotNull();
                            assertThat(e.getServerErrorMessage().getConstraint()).isEqualTo(cockroach ? "primary" : "items_pkey");
                            assertThat(e.getServerErrorMessage().getDetail()).isEqualTo("Key (item_id)=(123) already exists.");
                        });
            }

            try (Statement statement = connection.createStatement()) {
                assertThatThrownBy(() -> statement.execute("INSERT INTO items VALUES (789, 'Apple')"))
                        .isInstanceOfSatisfying(PSQLException.class, e -> {
                            assertThat(e.getServerErrorMessage()).isNotNull();
                            assertThat(e.getServerErrorMessage().getConstraint()).isEqualTo("items_name_key");
                            assertThat(e.getServerErrorMessage().getDetail()).isEqualTo(
                                    cockroach ? "Key (name)=('Apple') already exists." : "Key (name)=(Apple) already exists.");
                        });
            }

            try (Statement statement = connection.createStatement()) {
                assertThatThrownBy(() -> statement.execute("INSERT INTO items VALUES (789, 'Grape', 3, 4)"))
                        .isInstanceOfSatisfying(PSQLException.class, e -> {
                            assertThat(e.getServerErrorMessage()).isNotNull();
                            assertThat(e.getServerErrorMessage().getConstraint()).isEqualTo("items_a_b_key");
                            assertThat(e.getServerErrorMessage().getDetail()).isEqualTo(
                                    cockroach ? "Key (a,b)=(3,4) already exists." : "Key (a, b)=(3, 4) already exists.");
                        });
            }
        }
    }
}
