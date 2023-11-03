import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class Main {

    private final String url = "jdbc:postgresql://127.0.0.1:5588/training_hw_5";
    private final String user = "postgres";
    private final String password = "postgres";

    private final String creatingTable = """
            CREATE TABLE IF NOT EXISTS audit_department (
            revision_number VARCHAR(255) PRIMARY KEY,
            type VARCHAR(255) ,
            address VARCHAR(255) 
            )""";

    private final String insertData = """
            INSERT INTO audit_department (revision_number, type, address)
            VALUES (?, ?, ?)
            """;

    private final String getOne = """
            SELECT * FROM audit_department WHERE revision_number = ?
            """;

    private final String getAll = """
            SELECT * FROM audit_department
            """;

    public Connection connect() {
        Connection conn = null;
        try {
            conn = DriverManager.getConnection(url, user, password);
            System.out.println("Connected to the PostgreSQL server successfully.");
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }

        return conn;
    }

    public boolean createDB(Connection connect) {
        try {
            Statement createDbStatement = connect.createStatement();
            createDbStatement.execute(this.creatingTable);
            createDbStatement.close();
            return true;
        } catch (SQLException e) {
            System.out.println("Error occured: " + e.getMessage());
            return false;
        }
    }

    public boolean createAudit(Connection connection, String rn, String t, String a) {
        try {
            PreparedStatement insertingStatement = connection.prepareStatement(this.insertData,
                    Statement.RETURN_GENERATED_KEYS);
            insertingStatement.setString(1, rn);
            insertingStatement.setString(2, t);
            insertingStatement.setString(3, a);
            int d = insertingStatement.executeUpdate();
            if (d > 0) {
                System.out.printf("Data inserted, %d\n", d);
                return true;
            } else {
                System.out.println("Data not inserted");
                return true;
            }
        } catch (SQLException e) {
            System.out.println("Error occured: " + e.getMessage());
            return false;
        }
    }

    public String[] getOneByID(Connection connection, String ID) {
        try {
            PreparedStatement queryingState = connection.prepareStatement(this.getOne);
            queryingState.setString(1, ID);
            ResultSet rs = queryingState.executeQuery();
            while (rs.next()) {
                if (!rs.getString("revision_number").isBlank()) {
                    String[] prev = new String[3];
                    prev[0] = rs.getString("revision_number");
                    prev[1] = rs.getString("type");
                    prev[2] = rs.getString("address");
                    return prev;
                } else {
                    System.out.println("Data not queried");
                }
            }
            return null;
        } catch (SQLException e) {
            System.out.println("Error occured: " + e.getMessage());
            return null;
        }
    }

    public List<String[]> getAll(Connection connection) {
        try {
            Statement getAllStmt = connection.createStatement();
            ResultSet rs = getAllStmt.executeQuery(getAll);
            List<String[]> res = new ArrayList<String[]>();
            while (rs.next()) {
                String[] prev = new String[3];
                prev[0] = rs.getString("revision_number");
                prev[1] = rs.getString("type");
                prev[2] = rs.getString("address");
                res.add(prev);
            }
            return res;
        } catch (SQLException e) {
            System.out.println("Error occured: " + e.getMessage());
            return null;
        }

    }

    public static void main(String[] args) {
        Main main = new Main();
        Connection connection = main.connect();
        if (main.createDB(connection)) {
            String getterID = "Xg_8418";
            //Первоначально ввел в базу эти данные

//            String[] rev1 = {"Xg_5124", "Компьютер", "Общага"};
//            String[] rev2 = {"Xg_8418", "Телевизор", "ГУК"};
//            String[] rev3 = {"Xg_2110", "БЭСМ", "МАТИ"};
//            main.createAudit(connection, rev1[0], rev1[1], rev1[2]);
//            main.createAudit(connection, rev2[0], rev2[1], rev2[2]);
//            main.createAudit(connection, rev3[0], rev3[1], rev3[2]);

//            Далее получаем какой либо столбец по UID и выводим его

            System.out.println("Query for " + getterID);
            for (String elem : main.getOneByID(connection, getterID)) {
                System.out.printf("| %s ", elem);
            }

//            Реализуем вывод всей базы
            System.out.println("\nAll table:");
            for (String[] elem : main.getAll(connection)) {
                System.out.printf("| %s | %s | %s |\n", elem[0], elem[1], elem[2]);
            }
        }
    }
}