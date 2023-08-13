package repository;

import clients.avro.Product;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class ProductRepositoryMySQL {
    private static final String DATABASE = "cms_db";
    private static final String MYSQL_URL = "jdbc:mysql://localhost:3306/"+DATABASE;
    private static final String USERNAME = "root";
    private static final String PASSWORD = "root";

    private static final String INSERT_SQL = "INSERT INTO products (pogid, supc, price, quantity) VALUES (?, ?, ?, ?)";


    public void insertProduct(Product product){
        try (Connection conn = DriverManager.getConnection(MYSQL_URL, USERNAME, PASSWORD);
             PreparedStatement ps = conn.prepareStatement(INSERT_SQL)){
            ps.setLong(1, product.getPogId());
            ps.setString(2, product.getSupc());
            ps.setFloat(3, product.getPrice());
            ps.setInt(4, product.getQuantity());
            ps.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }


}
