package main;

import java.sql.*;

public class Conexion {

    private String url = "jdbc:mysql://localhost:3306/prueba";
    private String user = "root";
    private String password = "1234";
    private Connection con;

public Conexion() throws SQLException {
        con = DriverManager.getConnection(url, user, password);

    }

    public void ejecutarOperacionDML(String sql) throws SQLException {
        Statement stmt = con.createStatement();

        int n = stmt.executeUpdate(sql);
        if (n > 0) {
            System.out.println("Se ha realizado la operación DML correctamente");
        }
        stmt.close();
    }

    public void cerrarConexion() throws SQLException {
            con.close();
            System.out.println("Conexión cerrada");
        }
    }

