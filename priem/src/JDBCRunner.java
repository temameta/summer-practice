

import java.sql.*;

public class JDBCRunner {

    private static final String PROTOCOL = "jdbc:postgresql://";        // URL-prefix
    private static final String DRIVER = "org.postgresql.Driver";       // Driver name
    private static final String URL_LOCALE_NAME = "localhost/";         // ваш компьютер + порт по умолчанию

    private static final String DATABASE_NAME = "priem";          // FIXME имя базы

    public static final String DATABASE_URL = PROTOCOL + URL_LOCALE_NAME + DATABASE_NAME;
    public static final String USER_NAME = "postgres";                  // FIXME имя пользователя
    public static final String DATABASE_PASS = "postgres";              // FIXME пароль базы данных

    public static void main(String[] args) {

        // проверка возможности подключения
        checkDriver();
        checkDB();
        System.out.println("Подключение к базе данных | " + DATABASE_URL + "\n");

        // попытка открыть соединение с базой данных, которое java-закроет перед выходом из try-with-resources
        try (Connection connection = DriverManager.getConnection(DATABASE_URL, USER_NAME, DATABASE_PASS)) {
            //TODO show all tables
            getPortfolios(connection);
            System.out.println();
            getStudents(connection);
            System.out.println();
            getSpecialities(connection);
            System.out.println();
            getStatuses(connection);
            System.out.println();
            getFirstSevenPortfolios(connection);
            System.out.println();
            getNamesOfAdultStudents(connection);
            System.out.println();
            getPortfoliosSortedByStatusId(connection);
            System.out.println();

            //TODO show with param

            getStudentsByGender(connection, "М");
            System.out.println();
            getStudentsByGender(connection, "Ж");
            System.out.println();
//            getVillainNamed(connection, "Грю", false); System.out.println();// возьмем всех и найдем перебором
//            getVillainNamed(connection, "Грю", true); System.out.println(); // тоже самое сделает БД
//            getVillainMinions(connection, "Грю"); System.out.println();
//
//            // TODO correction
            addSpeciality(connection, "Аэронавигация", 34);
            System.out.println();
            removePortfolio(connection, 4, 7); System.out.println();
            correctStatus(connection, "Не принят", 2);

        } catch (SQLException e) {
            // При открытии соединения, выполнении запросов могут возникать различные ошибки
            // Согласно стандарту SQL:2008 в ситуациях нарушения ограничений уникальности (в т.ч. дублирования данных) возникают ошибки соответствующие статусу (или дочерние ему): SQLState 23000 - Integrity Constraint Violation
            if (e.getSQLState().startsWith("23")) {
                System.out.println("Произошло дублирование данных");
            } else throw new RuntimeException(e);
        }
    }

    // region // Проверка окружения и доступа к базе данных

    public static void checkDriver() {
        try {
            Class.forName(DRIVER);
        } catch (ClassNotFoundException e) {
            System.out.println("Нет JDBC-драйвера! Подключите JDBC-драйвер к проекту согласно инструкции.");
            throw new RuntimeException(e);
        }
    }

    public static void checkDB() {
        try {
            Connection connection = DriverManager.getConnection(DATABASE_URL, USER_NAME, DATABASE_PASS);
        } catch (SQLException e) {
            System.out.println("Нет базы данных! Проверьте имя базы, путь к базе или разверните локально резервную копию согласно инструкции");
            throw new RuntimeException(e);
        }
    }

    // endregion

    // region // SELECT-запросы без параметров в одной таблице

    private static void getPortfolios(Connection connection) throws SQLException {
        // имена столбцов
        String columnName0 = "id", columnName1 = "student_id", columnName2 = "status_id", columnName3 = "speciality_id";
        // значения ячеек
        int param0 = -1, param1 = -1, param2 = -1, param3 = -1;

        Statement statement = connection.createStatement();     // создаем оператор для простого запроса (без параметров)
        ResultSet rs = statement.executeQuery("SELECT * FROM portfolios;"); // выполняем запроса на поиск и получаем список ответов

        while (rs.next()) {  // пока есть данные, продвигаться по ним
            param3 = rs.getInt(columnName3);
            param2 = rs.getInt(columnName2); // значение ячейки, можно получить по имени; по умолчанию возвращается строка
            param1 = rs.getInt(columnName1);
            param0 = rs.getInt(columnName0);    // если точно уверены в типе данных ячейки, можно его сразу преобразовать
            System.out.println(param0 + " | " + param1 + " | " + param2 + " | " + param3);
        }
    }

    private static void getSpecialities(Connection connection) throws SQLException {
        // имена столбцов
        String columnName0 = "id", columnName1 = "code", columnName2 = "name";
        // значения ячеек
        int param0 = -1, param1 = -1;
        String param2 = null;

        Statement statement = connection.createStatement();     // создаем оператор для простого запроса (без параметров)
        ResultSet rs = statement.executeQuery("SELECT * FROM specialities;"); // выполняем запроса на поиск и получаем список ответов

        while (rs.next()) {  // пока есть данные, продвигаться по ним
            param2 = rs.getString(columnName2);
            param1 = rs.getInt(columnName1); // значение ячейки, можно получить по имени; по умолчанию возвращается строка
            param0 = rs.getInt(columnName0);    // если точно уверены в типе данных ячейки, можно его сразу преобразовать
            System.out.println(param0 + " | " + param1 + " | " + param2);
        }
    }

    private static void getStudents(Connection connection) throws SQLException {
        // имена столбцов
        String columnName0 = "id", columnName1 = "second_name", columnName2 = "name", columnName3 = "surname", columnName4 = "gender", columnName5 = "year_of_birth";
        // значения ячеек
        int param0 = -1, param5 = -1;
        String param1 = null, param2 = null, param3 = null, param4 = null;

        Statement statement = connection.createStatement();     // создаем оператор для простого запроса (без параметров)
        ResultSet rs = statement.executeQuery("SELECT * FROM students;"); // выполняем запроса на поиск и получаем список ответов

        while (rs.next()) {  // пока есть данные, продвигаться по ним
            param5 = rs.getInt(columnName5);
            param4 = rs.getString(columnName4);
            param3 = rs.getString(columnName3);
            param2 = rs.getString(columnName2); // значение ячейки, можно получить по имени; по умолчанию возвращается строка
            param1 = rs.getString(columnName1);
            param0 = rs.getInt(columnName0);    // если точно уверены в типе данных ячейки, можно его сразу преобразовать
            System.out.println(param0 + " | " + param1 + " | " + param2 + " | " + param3 + " | " + param4 + " | " + param5);
        }
    }

    private static void getStatuses(Connection connection) throws SQLException {
        // имена столбцов
        String columnName0 = "id", columnName1 = "name";
        // значения ячеек
        int param0 = -1;
        String param1 = null;

        Statement statement = connection.createStatement();     // создаем оператор для простого запроса (без параметров)
        ResultSet rs = statement.executeQuery("SELECT * FROM statuses;"); // выполняем запроса на поиск и получаем список ответов

        while (rs.next()) {  // пока есть данные, продвигаться по ним
            param1 = rs.getString(columnName1); // значение ячейки, можно получить по имени; по умолчанию возвращается строка
            param0 = rs.getInt(columnName0);    // если точно уверены в типе данных ячейки, можно его сразу преобразовать
            System.out.println(param0 + " | " + param1);
        }
    }
    // endregion
    // region // SELECT-запросы с параметрами и объединением таблиц

    private static void getFirstSevenPortfolios(Connection connection) throws SQLException {
        // имена столбцов
        String columnName0 = "id", columnName1 = "student_id", columnName2 = "status_id", columnName3 = "speciality_id";
        // значения ячеек
        int param0 = -1, param1 = -1, param2 = -1, param3 = -1;

        Statement statement = connection.createStatement();     // создаем оператор для простого запроса (без параметров)
        ResultSet rs = statement.executeQuery("SELECT * FROM portfolios LIMIT 7;"); // выполняем запроса на поиск и получаем список ответов

        while (rs.next()) {  // пока есть данные, продвигаться по ним
            param3 = rs.getInt(columnName3);
            param2 = rs.getInt(columnName2); // значение ячейки, можно получить по имени; по умолчанию возвращается строка
            param1 = rs.getInt(columnName1);
            param0 = rs.getInt(columnName0);    // если точно уверены в типе данных ячейки, можно его сразу преобразовать
            System.out.println(param0 + " | " + param1 + " | " + param2 + " | " + param3);
        }
    }

    private static void getNamesOfAdultStudents(Connection connection) throws SQLException {
        // имена столбцов
        String columnName0 = "full_name";
        // значения ячеек
        String param0 = null;

        PreparedStatement statement = connection.prepareStatement("SELECT second_name || ' ' || name || ' ' || surname " +
                "AS full_name " +
                "FROM students " +
                "WHERE year_of_birth < 2006;"); // выполняем запроса на поиск и получаем список ответов
        ResultSet rs = statement.executeQuery();

        while (rs.next()) {  // пока есть данные, продвигаться по ним
            param0 = rs.getString(columnName0);    // если точно уверены в типе данных ячейки, можно его сразу преобразовать
            System.out.println(param0);
        }
    }

    private static void getPortfoliosSortedByStatusId(Connection connection) throws SQLException {
        String columnName0 = "id", columnName1 = "student_id", columnName2 = "status_id", columnName3 = "speciality_id";
        int param0 = -1, param1 = -1, param2 = -1, param3 = -1;
        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery("SELECT * FROM portfolios " +
                "ORDER BY status_id ASC;");
        while (rs.next()) {
            param3 = rs.getInt(columnName3);
            param2 = rs.getInt(columnName2);
            param1 = rs.getInt(columnName1);
            param0 = rs.getInt(columnName0);
            System.out.println(param0 + " | " + param1 + " | " + param2 + " | " + param3);
        }
    }

    private static void getStudentsByGender(Connection connection, String gender) throws SQLException {
        if (gender == null || gender.isBlank()) return;
        // имена столбцов
        String columnName0 = "id", columnName1 = "second_name", columnName2 = "name", columnName3 = "surname", columnName4 = "gender", columnName5 = "year_of_birth";
        // значения ячеек
        int param0 = -1, param5 = -1;
        String param1 = null, param2 = null, param3 = null, param4 = null;

        PreparedStatement statement = connection.prepareStatement("SELECT * " +
                "FROM students " +
                "WHERE gender LIKE ?;"); // выполняем запроса на поиск и получаем список ответов
        statement.setString(1, gender);
        ResultSet rs = statement.executeQuery();

        while (rs.next()) {  // пока есть данные, продвигаться по ним
            param5 = rs.getInt(columnName5);
            param4 = rs.getString(columnName4);
            param3 = rs.getString(columnName3);
            param2 = rs.getString(columnName2); // значение ячейки, можно получить по имени; по умолчанию возвращается строка
            param1 = rs.getString(columnName1);
            param0 = rs.getInt(columnName0);    // если точно уверены в типе данных ячейки, можно его сразу преобразовать
            System.out.println(param0 + " | " + param1 + " | " + param2 + " | " + param3 + " | " + param4 + " | " + param5);
        }
    }

    private static void addSpeciality(Connection connection, String name, int code) throws SQLException {
        if (name == null || name.isBlank() || code < 20) return;

        PreparedStatement statement = connection.prepareStatement(
                "INSERT INTO specialities(code, name) VALUES (?, ?) returning id;", Statement.RETURN_GENERATED_KEYS);
        statement.setInt(1, code);
        statement.setString(2, name);

        int count = statement.executeUpdate();

        ResultSet rs = statement.getGeneratedKeys();
        if (rs.next()) {
            System.out.println("Идентификатор специальности " + rs.getInt(1));
        }

        System.out.println("INSERTed " + count + " specialities");
        getSpecialities(connection);
    }
    private static void removePortfolio(Connection connection, int firstId, int secondId) throws SQLException {
        if (firstId < 0 || secondId < 0) return;

        PreparedStatement statement = connection.prepareStatement("DELETE FROM portfolios WHERE id=? OR id=?;");
        statement.setInt(1, firstId);
        statement.setInt(2, secondId);

        int count = statement.executeUpdate(); // выполняем запрос на удаление и возвращаем количество измененных строк
        System.out.println("DELETEd " + count + " portfolios");
        getPortfolios(connection);
    }
    private static void correctStatus(Connection connection, String name, int id) throws SQLException {
        if (name == null || name.isBlank() || id < 0) return;

        PreparedStatement statement = connection.prepareStatement("UPDATE statuses SET name=? WHERE id=?;");
        statement.setString(1, name); // сначала что передаем
        statement.setInt(2, id);   // затем по чему ищем

        int count = statement.executeUpdate();  // выполняем запрос на коррекцию и возвращаем количество измененных строк

        System.out.println("UPDATEd " + count + " statuses");
        getStatuses(connection);
    }
}
