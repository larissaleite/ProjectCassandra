package br.com.cassandra.dao;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

public class EstoqueClient {
	
	private Session session;
	private Cluster cluster;
	
	public void createSchema() { 
		session.execute("CREATE KEYSPACE IF NOT EXISTS estoque WITH replication " + 
			      "= {'class':'SimpleStrategy', 'replication_factor':2};");
		
		session.execute(
			      "CREATE TABLE IF NOT EXISTS estoque.produtos (" +
			            "id uuid PRIMARY KEY," + 
			            "nome text," + 
			            "preco float,"+
			            "tags set<text>,"
			            + ");"
			       );
		
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
			
			//session.execute("ALTER TABLE estoque.produtos ADD tags set<text>;");
	}
	
	public void loadData() { 
		session.execute(
			      "INSERT INTO estoque.produtos (id, nome, preco, tags) " +
			      "VALUES (" +
			          "756716f7-2e54-4715-9f00-91dcbea6cf50," +
			          "'Monitor'," +
			          "200," +
			          "{'eletronicos', 'computador', 'escritorio'})" +
			          ";");
		
		session.execute(
			      "INSERT INTO estoque.produtos (id, nome, preco, tags) " +
			      "VALUES (" +
			          "755516f7-2e54-6314-9f00-91dcbea6cf50," +
			          "'Caderno'," +
			          "10," +
			          "{'escritorio'})" +
			          ";");
		
			session.execute(
			      "INSERT INTO estoque.recebimentos (id, produto_id, quantidade, mes) " +
			      "VALUES (" +
			          "2cc9ccb7-6221-4ccb-8387-f22b6a1b354d," +
			          "756716f7-2e54-4715-9f00-91dcbea6cf50," +
			          "100," +
			          "5" +
			          ");");
	}
	
	public void querySchema() {
		ResultSet results = session.execute("SELECT * FROM estoque.produtos;");
		        //"WHERE id = 2cc9ccb7-6221-4ccb-8387-f22b6a1b354d;");
		
		System.out.println(String.format("%-30s\t%-20s\t%-20s\n%s", "id", "nome", "preco",
			       "-----------------------------------+-----------------------+--------------------"));
		for (Row row : results) {
			System.out.println(String.format("%-30s\t%-20s\t%-20s", row.getUUID("id"),
			row.getString("nome"),  row.getFloat("preco")));
		}
	}
	
	public void connect(String node) {
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
	}

	public void close() {
		cluster.close();	
	}

	public static void main(String[] args) {
		EstoqueClient client = new EstoqueClient();
		client.connect("127.0.0.1");
		//client.createSchema();
		//client.loadData();
		client.querySchema();
		client.close();	

	}

}
