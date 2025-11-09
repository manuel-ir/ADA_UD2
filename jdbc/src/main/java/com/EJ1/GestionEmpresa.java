package com.EJ1;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class GestionEmpresa {

    public static void main(String[] args) {

        String url = "jdbc:mysql://localhost:3306/EMPRESA";
        String user = "root";
        String password = "1234";

        Connection con = null;

        try {
            con = DriverManager.getConnection(url, user, password);
            System.out.println("Conexión establecida con la BD 'EMPRESA'.");

            consultaA(con);
            consultaB(con);
            consultaC(con);
            consultaD(con);
            consultaE(con);
            consultaF(con);
            consultaG(con);
            consultaH(con);
            consultaI(con);
            consultaJ(con);
            consultaK(con);
            consultaL(con);

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

    // CONSULTA "A"
    /**
     * A. Hallar la comisión, el nombre y el salario de los empleados con más de
     * tres hijos, ordenados por comisión y, dentro de comisión, alfabéticamente.
     */
    public static void consultaA(Connection con) {
        String sql = "SELECT Comision, Nombre, Salario FROM EMPLEADOS " +
                     "WHERE Num_hijos > 3 " +
                     "ORDER BY Comision, Nombre";

        System.out.println("\nA. Empleados con más de 3 hijos");
        
        try (Statement stmt = con.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {

            while (rs.next()) {
                
                String comisionStr = rs.getString("Comision");
                
                String nombre = rs.getString("Nombre");
                double salario = rs.getDouble("Salario");

              
                System.out.println("Nombre: " + nombre + " | Salario: " + salario + " | Comisión: " + (comisionStr == null ? "N/A" : comisionStr));
            }
        } catch (SQLException e) {
            System.err.println("Error en la consulta A: " + e.getMessage());
        }
    }

    // CONSULTA "B"
    /**
     * B. Obtener los nombres de los departamentos que no dependen de otros.
     */
    public static void consultaB(Connection con) {
        String sql = "SELECT Nombre FROM DEPARTAMENTOS WHERE Depto_jefe IS NULL";

        System.out.println("\nB. Departamentos que no dependen de otros");
        try (Statement stmt = con.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {

            while (rs.next()) {
                System.out.println("Nombre: " + rs.getString("Nombre"));
            }
        } catch (SQLException e) {
            System.err.println("Error en la consulta B: " + e.getMessage());
        }
    }

    // CONSULTA "C"
    /**
     * C. Obtener, por orden alfabético, los nombres y los salarios de los
     * empleados cuyo salario esté comprendido entre 1250 y 1300 euros.
     */
    public static void consultaC(Connection con) {
        
        String sql = "SELECT Nombre, Salario FROM EMPLEADOS " +
                     "WHERE Salario BETWEEN ? AND ? " +
                     "ORDER BY Nombre";

        System.out.println("\nC. Empleados con salario entre 1250 y 1300");
        try (PreparedStatement ps = con.prepareStatement(sql)) {

            // Seteamos los parámetros
            ps.setDouble(1, 1250.0);
            ps.setDouble(2, 1300.0);

            
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    
                    System.out.println("Nombre: " + rs.getString("Nombre") + " | Salario: " + rs.getDouble("Salario"));
                }
            }
        } catch (SQLException e) {
            System.err.println("Error en la consulta C: " + e.getMessage());
        }
    }

    // CONSULTA "D"
    /**
     * D. Datos de los empleados que cumplen la condición anterior o tienen al menos
     * un hijo.
     */
    public static void consultaD(Connection con) {
        String sql = "SELECT Cod, Nombre, Salario, Num_hijos FROM EMPLEADOS " +
                     "WHERE (Salario BETWEEN ? AND ?) OR (Num_hijos >= ?)";

        System.out.println("\nD. Empleados (salario 1250-1300) O (con hijos)");
        try (PreparedStatement ps = con.prepareStatement(sql)) {

            // Seteamos los parámetros
            ps.setDouble(1, 1250.0);
            ps.setDouble(2, 1300.0);
            ps.setInt(3, 1);

            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    // Imprimimos con println simple
                    System.out.println("Cod: " + rs.getInt("Cod") +
                                       " | Nombre: " + rs.getString("Nombre") +
                                       " | Salario: " + rs.getDouble("Salario") +
                                       " | Hijos: " + rs.getInt("Num_hijos"));
                }
            }
        } catch (SQLException e) {
            System.err.println("Error en la consulta D: " + e.getMessage());
        }
    }

    // CONSULTA "E"
    /**
     * E. Obtener, por orden alfabético, los nombres de los departamentos que no
     * contengan la palabra 'Dirección' ni 'Sector'.
     */
    public static void consultaE(Connection con) {
        String sql = "SELECT Nombre FROM DEPARTAMENTOS " +
                     "WHERE Nombre NOT LIKE '%Dirección%' AND Nombre NOT LIKE '%Sector%' " +
                     "ORDER BY Nombre";

        System.out.println("\nE. Deptos sin 'Dirección' ni 'Sector'");
        try (Statement stmt = con.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {

            while (rs.next()) {
                System.out.println("Nombre: " + rs.getString("Nombre"));
            }
        } catch (SQLException e) {
            System.err.println("Error en la consulta E: " + e.getMessage());
        }
    }

    // CONSULTA "F"
    /**
     * F. Deptos que, o bien tienen directores en 'F' y presupuesto <= 5, o bien
     * no dependen de ningún otro.
     */
    public static void consultaF(Connection con) {
        String sql = "SELECT Nombre, Tipo_dir, Presupuesto, Depto_jefe FROM DEPARTAMENTOS " +
                     "WHERE (Tipo_dir = ? AND Presupuesto <= ?) OR (Depto_jefe IS NULL) " +
                     "ORDER BY Nombre";

        System.out.println("\nF. Deptos (Dir='F' y Pres<=5) O (Sin Jefe)");
        try (PreparedStatement ps = con.prepareStatement(sql)) {

            ps.setString(1, "F");
            ps.setInt(2, 5); // El presupuesto está en miles, así que 5 = 5 mil

            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    
                    String deptoJefeStr = rs.getString("Depto_jefe");
                    
                    if (deptoJefeStr == null) {
                        deptoJefeStr = "N/A";
                    }
                    
                    System.out.println("Nombre: " + rs.getString("Nombre") +
                                       " | Tipo Dir: " + rs.getString("Tipo_dir") +
                                       " | Presupuesto: " + rs.getDouble("Presupuesto") +
                                       " | Jefe: " + deptoJefeStr);
                }
            }
        } catch (SQLException e) {
            System.err.println("Error en la consulta F: " + e.getMessage());
        }
    }

    // CONSULTA "G"
    /**
     * G. Hallar, por orden de número de empleado, el nombre y el salario total
     * (salario más comisión) de los empleados cuyo salario total supera los 1300
     * euros.
     */
    public static void consultaG(Connection con) {
        
        String sql = "SELECT Cod, Nombre, Salario, Comision FROM EMPLEADOS ORDER BY Cod";

        System.out.println("\nG. Empleados con Salario Total > 1300 (calculado en Java)");
        
        
        try (Statement stmt = con.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {

            while (rs.next()) {
                
                int cod = rs.getInt("Cod");
                String nombre = rs.getString("Nombre");
                double salario = rs.getDouble("Salario");
                
                // Leemos la comisión como String para comprobar si es null
                String comisionStr = rs.getString("Comision");
                
                double comision = 0.0;
                // Si el String no es null, lo convertimos a double
                if (comisionStr != null) {
                    comision = Double.parseDouble(comisionStr);
                }
                
                // Ahora la suma es segura. Si era null, sumará 0.0
                double salarioTotal = salario + comision;

                
                if (salarioTotal > 1300) {
                   
                    System.out.println("Cod: " + cod + " | Nombre: " + nombre + " | Salario Total: " + salarioTotal);
                }
            }
        } catch (SQLException e) {
            System.err.println("Error en la consulta G: " + e.getMessage());
        }
    }

    // CONSULTA "H"
    /**
     * H. Hallar el número de empleados de toda la empresa.
     */
    public static void consultaH(Connection con) {
        String sql = "SELECT COUNT(*) FROM EMPLEADOS";

        System.out.println("\nH. Número total de empleados");
        try (Statement stmt = con.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {

            
            if (rs.next()) {
                int totalEmpleados = rs.getInt(1); 
                System.out.println("Número total de empleados: " + totalEmpleados);
            }
        } catch (SQLException e) {
            System.err.println("Error en la consulta H: " + e.getMessage());
        }
    }

    // CONSULTA "I"
    /**
     * I. Hallar cuántos departamentos existen y el presupuesto anual medio.
     */
    public static void consultaI(Connection con) {
        String sql = "SELECT COUNT(*), AVG(Presupuesto) FROM DEPARTAMENTOS";

        System.out.println("\nI. Resumen de Departamentos");
        try (Statement stmt = con.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {

            
            if (rs.next()) {
                int totalDeptos = rs.getInt(1);
                double presupuestoMedio = rs.getDouble(2);
                // Imprimimos con println simple
                System.out.println("Número de departamentos: " + totalDeptos);
                System.out.println("Presupuesto medio: " + presupuestoMedio + " miles de euros");
            }
        } catch (SQLException e) {
            System.err.println("Error en la consulta I: " + e.getMessage());
        }
    }

    // CONSULTA "J"
    /**
     * J. Hallar el número de empleados y de extensiones telefónicas distintas del
     * departamento 112.
     */
    public static void consultaJ(Connection con) {
        String sql = "SELECT COUNT(*), COUNT(DISTINCT Telefono) FROM EMPLEADOS " +
                     "WHERE Departamento = ?";

        System.out.println("\nJ. Resumen Depto 112");
        try (PreparedStatement ps = con.prepareStatement(sql)) {

            ps.setInt(1, 112);

            try (ResultSet rs = ps.executeQuery()) {
                
                if (rs.next()) {
                    int totalEmpleados = rs.getInt(1);
                    int telefonosDistintos = rs.getInt(2);
                    
                    System.out.println("Empleados en Depto 112: " + totalEmpleados);
                    System.out.println("Teléfonos distintos en Depto 112: " + telefonosDistintos);
                }
            }
        } catch (SQLException e) {
            System.err.println("Error en la consulta J: " + e.getMessage());
        }
    }

    // CONSULTA "K"
    /**
     * K. Códigos de los departamentos que no hacen de departamento jefe.
     */
    public static void consultaK(Connection con) {
        // Un depto no es jefe si su "Número" no está en la columna 'Depto_jefe'
        String sql = "SELECT Numero, Nombre FROM DEPARTAMENTOS " +
                     "WHERE Numero NOT IN ( " +
                     "  SELECT DISTINCT Depto_jefe FROM DEPARTAMENTOS WHERE Depto_jefe IS NOT NULL " +
                     ")";

        System.out.println("\nK. Departamentos que NO son jefes");
        try (Statement stmt = con.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {

            while (rs.next()) {
                
                System.out.println("Cod: " + rs.getInt("Numero") + " | Nombre: " + rs.getString("Nombre"));
            }
        } catch (SQLException e) {
            System.err.println("Error en la consulta K: " + e.getMessage());
        }
    }

    // CONSULTA "L"
    /**
     * L. Ídem pero que sí hacen de departamento jefe de algún otro departamento.
     */
    public static void consultaL(Connection con) {
        
        String sql = "SELECT DISTINCT d.Numero, d.Nombre FROM DEPARTAMENTOS d " +
                     "JOIN DEPARTAMENTOS d_hijos ON d.Numero = d_hijos.Depto_jefe " +
                     "ORDER BY d.Numero";

        System.out.println("\nL. Departamentos que SÍ son jefes");
        try (Statement stmt = con.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {

            while (rs.next()) {

                System.out.println("Cod: " + rs.getInt("Numero") + " | Nombre: " + rs.getString("Nombre"));
            }
        } catch (SQLException e) {
            System.err.println("Error en la consulta L: " + e.getMessage());
        }
    }
}