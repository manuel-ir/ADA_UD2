package com.EJ4;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;

public class GestionEjercicio4 {

    public static void main(String[] args) {

        String url = "jdbc:mysql://localhost:3306/EJERCICIO4"; 
        String user = "root";
        String password = "1234";

        Connection con = null;

        try {
            con = DriverManager.getConnection(url, user, password);
            System.out.println("Conexión establecida con la BD 'EJERCICIO4'.");

            // a) Tarifa entre 10 y 12
            consultaA(con, 10, 12);

            // b) Oficios en edificio 111
            consultaB(con, 111);

            // c) Trabajador y supervisor
            consultaC(con);

            // d) Trabajadores en oficinas
            consultaD(con);

            // e) Días de FONTANERO en edificio 312
            consultaE(con, "FONTANERO", 312);

            // f) Cuántos oficios hay
            consultaF(con);

            // g) Tarifa más alta por supervisor
            consultaG(con);
            
            // h) Tarifa más alta por supervisor (con > 1 trabajador)
            consultaH(con);

            // i) Tarifa menor que el promedio total
            consultaI(con);
            
            // j) Tarifa menor que el promedio de su oficio
            consultaJ(con);

            // k) Tarifa menor que el promedio de su supervisor
            consultaK(con);
            
            // l) Supervisores con trabajadores con tarifa > 15
            consultaL(con, 15);

        } catch (SQLException e) {
            System.err.println("Error fatal de SQL: " + e.getMessage());
            e.printStackTrace();
        } finally {
            if (con != null) {
                try {
                    con.close();
                    System.out.println("\nConexión cerrada.");
                } catch (SQLException e) {
                    System.err.println("Error al cerrar la conexión: " + e.getMessage());
                }
            }
        }
    }

    // a) Nombre de los trabajadores cuya tarifa este entre dos extremos.
    public static void consultaA(Connection con, double min, double max) {
        String sql = "{call sp_consulta_a(?, ?)}";
        System.out.println("\na. Trabajadores con tarifa entre " + min + " y " + max + ":");

        try (CallableStatement cs = con.prepareCall(sql)) {
            cs.setDouble(1, min);
            cs.setDouble(2, max);

            try (ResultSet rs = cs.executeQuery()) {
                while (rs.next()) {
                    System.out.println("  Nombre: " + rs.getString("NOMBRE") + " | Tarifa: " + rs.getDouble("TARIFA"));
                }
            }
        } catch (SQLException e) {
            System.err.println("Error en la consulta A: " + e.getMessage());
        }
    }

    // b) ¿Cuáles son los oficios de los trabajadores asignados un edificio?
    public static void consultaB(Connection con, int idEdificio) {
        String sql = "{call sp_consulta_b(?)}";
        System.out.println("\nb. Oficios en el edificio " + idEdificio + ":");

        try (CallableStatement cs = con.prepareCall(sql)) {
            cs.setInt(1, idEdificio);

            try (ResultSet rs = cs.executeQuery()) {
                while (rs.next()) {
                    System.out.println("  Oficio: " + rs.getString("OFICIO"));
                }
            }
        } catch (SQLException e) {
            System.err.println("Error en la consulta B: " + e.getMessage());
        }
    }

    // c) Indicar el nombre del trabajador y el de su supervisor.
    public static void consultaC(Connection con) {
        String sql = "{call sp_consulta_c()}";
        System.out.println("\nc. Trabajador y su supervisor:");

        try (CallableStatement cs = con.prepareCall(sql);
             ResultSet rs = cs.executeQuery()) {
            
            while (rs.next()) {
                System.out.println("  Trabajador: " + rs.getString("Trabajador") + " | Supervisor: " + rs.getString("Supervisor"));
            }
        } catch (SQLException e) {
            System.err.println("Error en la consulta C: " + e.getMessage());
        }
    }

    // d) Nombre de los trabajadores asignados a oficinas.
    public static void consultaD(Connection con) {
        String sql = "{call sp_consulta_d()}";
        System.out.println("\nd. Trabajadores asignados a 'OFICINA':");

        try (CallableStatement cs = con.prepareCall(sql);
             ResultSet rs = cs.executeQuery()) {
            
            while (rs.next()) {
                System.out.println("  Nombre: " + rs.getString("NOMBRE"));
            }
        } catch (SQLException e) {
            System.err.println("Error en la consulta D: " + e.getMessage());
        }
    }

    // e) ¿Cuál es el número total de días que se han dedicado a una actividad (oficio)
    //    en un edificio concreto?
    public static void consultaE(Connection con, String oficio, int idEdificio) {
        String sql = "{call sp_consulta_e(?, ?)}";
        System.out.println("\ne. Total de días de '" + oficio + "' en edificio " + idEdificio + ":");

        try (CallableStatement cs = con.prepareCall(sql)) {
            cs.setString(1, oficio);
            cs.setInt(2, idEdificio);

            try (ResultSet rs = cs.executeQuery()) {
                if (rs.next()) {
                    System.out.println("  Total Días: " + rs.getInt("Total_Dias"));
                }
            }
        } catch (SQLException e) {
            System.err.println("Error en la consulta E: " + e.getMessage());
        }
    }

    // f) ¿Cuántos tipos de oficios diferentes hay?
    public static void consultaF(Connection con) {
        String sql = "{call sp_consulta_f()}";
        System.out.println("\nf. Número total de oficios diferentes:");

        try (CallableStatement cs = con.prepareCall(sql);
             ResultSet rs = cs.executeQuery()) {
            
            if (rs.next()) {
                System.out.println("  Total de Oficios: " + rs.getInt("Total_Oficios"));
            }
        } catch (SQLException e) {
            System.err.println("Error en la consulta F: " + e.getMessage());
        }
    }

    // g) Para cada supervisor, ¿Cuál es la tarifa por hora más alta?
    public static void consultaG(Connection con) {
        String sql = "{call sp_consulta_g()}";
        System.out.println("\ng. Tarifa más alta por supervisor:");

        try (CallableStatement cs = con.prepareCall(sql);
             ResultSet rs = cs.executeQuery()) {
            
            while (rs.next()) {
                System.out.println("  Supervisor: " + rs.getString("Supervisor") + " | Tarifa Máx: " + rs.getDouble("Tarifa_Max"));
            }
        } catch (SQLException e) {
            System.err.println("Error en la consulta G: " + e.getMessage());
        }
    }

    // h) Para cada supervisor que supervisa a más de un trabajador,
    //    ¿cuál es la tarifa más alta?
    public static void consultaH(Connection con) {
        String sql = "{call sp_consulta_h()}";
        System.out.println("\nh. Tarifa más alta por supervisor (que supervisa a > 1):");

        try (CallableStatement cs = con.prepareCall(sql);
             ResultSet rs = cs.executeQuery()) {
            
            while (rs.next()) {
                System.out.println("  Supervisor: " + rs.getString("Supervisor") + " | Tarifa Máx: " + rs.getDouble("Tarifa_Max"));
            }
        } catch (SQLException e) {
            System.err.println("Error en la consulta H: " + e.getMessage());
        }
    }

    // i) ¿Qué trabajadores reciben una tarifa por hora menor que la del promedio?
    public static void consultaI(Connection con) {
        String sql = "{call sp_consulta_i()}";
        System.out.println("\ni. Trabajadores con tarifa MENOR al promedio TOTAL:");

        try (CallableStatement cs = con.prepareCall(sql);
             ResultSet rs = cs.executeQuery()) {
            
            while (rs.next()) {
                System.out.println("  Nombre: " + rs.getString("NOMBRE") + " | Tarifa: " + rs.getDouble("TARIFA"));
            }
        } catch (SQLException e) {
            System.err.println("Error en la consulta I: " + e.getMessage());
        }
    }

    // j) ¿Qué trabajadores reciben una tarifa por hora menor que la del
    //    promedio de los trabajadores que tienen su mismo oficio?
    public static void consultaJ(Connection con) {
        String sql = "{call sp_consulta_j()}";
        System.out.println("\nj. Trabajadores con tarifa MENOR al promedio de su OFICIO:");

        try (CallableStatement cs = con.prepareCall(sql);
             ResultSet rs = cs.executeQuery()) {
            
            while (rs.next()) {
                System.out.println("  Nombre: " + rs.getString("NOMBRE") + " | Oficio: " + rs.getString("OFICIO") + " | Tarifa: " + rs.getDouble("TARIFA"));
            }
        } catch (SQLException e) {
            System.err.println("Error en la consulta J: " + e.getMessage());
        }
    }

    // k) ¿Qué trabajadores reciben una tarifa por hora menor que la del
    //    promedio de los trabajadores que dependen del mismo supervisor que él?
    public static void consultaK(Connection con) {
        String sql = "{call sp_consulta_k()}";
        System.out.println("\nk. Trabajadores con tarifa MENOR al promedio de su SUPERVISOR:");

        try (CallableStatement cs = con.prepareCall(sql);
             ResultSet rs = cs.executeQuery()) {
            
            while (rs.next()) {
                System.out.println("  Nombre: " + rs.getString("NOMBRE") + " | Supervisor: " + rs.getInt("ID_SUPV") + " | Tarifa: " + rs.getDouble("TARIFA"));
            }
        } catch (SQLException e) {
            System.err.println("Error en la consulta K: " + e.getMessage());
        }
    }

    // l) ¿Qué supervisores tienen trabajadores que tienen una tarifa por hora
    //    encima un determinado valor?
    public static void consultaL(Connection con, double tarifaMinima) {
        String sql = "{call sp_consulta_l(?)}";
        System.out.println("\nl. Supervisores con trabajadores con tarifa > " + tarifaMinima + ":");

        try (CallableStatement cs = con.prepareCall(sql)) {
            cs.setDouble(1, tarifaMinima);

            try (ResultSet rs = cs.executeQuery()) {
                while (rs.next()) {
                    System.out.println("  Supervisor: " + rs.getString("Supervisor"));
                }
            }
        } catch (SQLException e) {
            System.err.println("Error en la consulta L: " + e.getMessage());
        }
    }
}
