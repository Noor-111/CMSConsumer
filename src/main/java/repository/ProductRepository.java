package repository;

import clients.avro.Product;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.insert.RegularInsert;

public class ProductRepository {
    private static final String TABLE_NAME = "products";
    private static final String KEYSPACE= "cmskeyspace";
    private final CqlSession session;

    public ProductRepository(CqlSession session) {
        this.session = session;
    }
    public void insertProduct(Product product){
        session.execute("USE " + CqlIdentifier.fromCql(KEYSPACE));
        RegularInsert insertInto = QueryBuilder.insertInto(TABLE_NAME)
                .value("pogid", QueryBuilder.bindMarker())
                .value("supc", QueryBuilder.bindMarker())
                .value("brand", QueryBuilder.bindMarker())
                .value("category", QueryBuilder.bindMarker())
                .value("country", QueryBuilder.bindMarker())
                .value("description", QueryBuilder.bindMarker())
                .value("sellercode", QueryBuilder.bindMarker())
                .value("size", QueryBuilder.bindMarker())
                .value("subcategory", QueryBuilder.bindMarker());

        SimpleStatement insertStatement = insertInto.build();

        //insertStatement = insertStatement.setKeyspace(KEYSPACE);

        PreparedStatement preparedStatement = session.prepare(insertStatement);

        BoundStatement statement = preparedStatement.bind()
                .setLong("pogid", product.getPogId())
                .setString("supc", product.getSupc())
                .setString("brand", product.getBrand())
                .setString("category", product.getCategory())
                .setString("country", product.getCountry())
                .setString("description", product.getDescription())
                .setString("sellercode", product.getSellerCode())
                .setString("size",product.getSize())
                .setString("subcategory", product.getSubCategory());

        session.execute(statement);


    }
}
