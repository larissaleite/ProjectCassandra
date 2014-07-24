package br.com.cassandra.dao;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Session;

public class Configuration {
	
	private static Cluster cluster;
	private static Session session;
	
	private Configuration() {}
	
	private static void createSchema() {
		session.execute("CREATE KEYSPACE IF NOT EXISTS estoque WITH replication " + 
			      "= {'class':'SimpleStrategy', 'replication_factor':2};");
		
		session.execute(
			      "CREATE TABLE IF NOT EXISTS estoque.produtos (" +
			            "id uuid," + 
			            "nome text," + 
			            "preco float,"+
			            "tags set<text>," +
			            "PRIMARY KEY(id, nome)"
			            + ");"
			       );
		
		session.execute("CREATE INDEX produto_nome ON estoque.produtos (nome);");
		
			session.execute(
			      "CREATE TABLE IF NOT EXISTS estoque.vendas (" +
			            "id uuid PRIMARY KEY," +
			            "quantidade int," +
			            "mes int, " +
			            "produto_id uuid," +
			            ");"
			       );
			
			session.execute(
				      "CREATE TABLE IF NOT EXISTS estoque.recebimentos (" +
				            "id uuid PRIMARY KEY," +
				            "quantidade int," +
				            "mes int, " +
				            "produto_id uuid," +
				            ");"
				       );
	}
	
	private static Session connect(String node) {
		/*Adds a contact point and builds a cluster instance*/
		cluster = Cluster.builder()
		.addContactPoint(node)
		.build();
		
		/*Retrieves metadata from the cluster, such as its name, and for each node, the datacenter, address and rack*/
		Metadata metadata = cluster.getMetadata();
		System.out.printf("Connected to cluster: %s\n", 
		metadata.getClusterName());
		
		for ( Host host : metadata.getAllHosts() ) {
			System.out.printf("Datatacenter: %s; Host: %s; Rack: %s\n", host.getDatacenter(), host.getAddress(), host.getRack());
		}
		
		session = cluster.connect();
		createSchema();
		return session;
	}
	
	public static Session getSession() {
		if (session == null) {
			session = connect("127.0.0.1");
		}
		return session;
	}
	
	public static void close() {
		cluster.close();	
	}

}
