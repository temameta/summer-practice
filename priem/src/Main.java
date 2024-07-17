public class Main {
    public static void main(String[] args) {
        try {
            Class.forName("org.postgresql.Driver");
            System.out.println("Connected JDBC Driver");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("JDBC Driver is not found. Include it in your library path");
        }

    }
}
