package org.example;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.*;
import java.util.Properties;
import java.util.Scanner;

public class App {
    private static final int MAX_ROWS_TO_DISPLAY = 10;

    static Properties properties = new Properties();

    static {
        try (InputStream input = App.class.getResourceAsStream("/config.properties")) {
            properties.load(input);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static String getJdbcUrl() {
        return properties.getProperty("jdbcUrl");
    }

    private static String getJdbcUsername() {
        return properties.getProperty("jdbcUsername");
    }

    private static String getJdbcPassword() {
        return properties.getProperty("jdbcPassword");
    }


    public static void main(String[] args) throws SQLException {

        String jdbcUrl = getJdbcUrl();
        String jdbcUsername = getJdbcUsername();
        String jdbcPassword = getJdbcPassword();

        if (jdbcUrl == null || jdbcUsername == null || jdbcPassword == null) {
            System.err.println("Не удалось получить параметры подключения из config.properties");
            return;
        }
        try (Connection connection = DriverManager.getConnection(jdbcUrl, jdbcUsername, jdbcPassword)) {
            System.out.println("Соединение с базой данных успешно установлено!");
            Scanner console = new Scanner(System.in);
            String request;
            while (true) {
                System.out.println("Введите sql-команду: ");
                System.out.print("<");
                request = console.nextLine();

                if (request.equalsIgnoreCase("QUIT")) {
                    System.out.println("Введена команда QUIT.");
                    break;
                }

                try {
                    executeSql(connection, request);
                } catch (SQLException e) {
                    System.err.println("Ошибка выполнения SQL: " + e.getMessage());
                }
            }

            System.out.println("Завершение работы.");

        } catch (SQLException e) {
            System.err.println("Ошибка при подключении к базе данных: " + e.getMessage());
        }
    }

    private static void executeSql(Connection connection, String request) throws SQLException {
        String sqlRequest = request.trim().toUpperCase();

        if (sqlRequest.startsWith("SELECT")) {
            executeQuery(connection, sqlRequest);
        } else if (sqlRequest.startsWith("INSERT") || sqlRequest.startsWith("DELETE") || sqlRequest.startsWith("UPDATE")) {
            executeDML(connection, sqlRequest);
        } else if (sqlRequest.startsWith("CREATE") || sqlRequest.startsWith("DROP")) {
            executeDDL(connection, sqlRequest);
        } else {
            System.out.println("Разрешены только DML- и DDL-команды.");
        }
    }

    private static void executeQuery(Connection connection, String request) throws SQLException {
        try ( Statement statement = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);) {
            try (ResultSet resultSet = statement.executeQuery(request)) {
                int rowCount = 0;
                if (resultSet.last()) {
                    rowCount = resultSet.getRow();
                    resultSet.beforeFirst();
                }

                int displayedRows = 0;
                while (resultSet.next() && displayedRows < MAX_ROWS_TO_DISPLAY) {
                    ResultSetMetaData metaData = resultSet.getMetaData();
                    int columnCount = metaData.getColumnCount();
                    for (int i = 1; i <= columnCount; i++) {
                        System.out.print(metaData.getColumnName(i) + ": " + resultSet.getString(i) + "\t");
                    }
                    System.out.println();
                    displayedRows++;
                }

                if (rowCount > MAX_ROWS_TO_DISPLAY) {
                    System.out.println("В БД есть еще записи (" + rowCount + " всего).");
                }
            }

        }
    }

    private static void executeDML(Connection connection, String request) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            int updateStrings = statement.executeUpdate(request);
            System.out.println("Успешно выполнено. Изменено " + updateStrings + " строк.");
        }
    }

    private static void executeDDL(Connection connection, String request) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute(request);
            System.out.println("Успешно выполнено.");
        }
    }
}