package util;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

/**
 * Created on 22-Dec-17.
 */
public class CassandraConnector
{
    private Cluster cluster;
    private Session session;

    public void connect(final String node, final int port)
    {
        this.cluster = Cluster.builder()
                .addContactPoint(node).withPort(port).build();
        session = cluster.connect();
    }
    public Session getSession()
    {
        return this.session;
    }

    public void close()
    {
        cluster.close();
    }
}