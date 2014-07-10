package br.com.cassandra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

public class SimpleClient2 {
	
	private Session session;
	private Cluster cluster;
	
	public void createSchema() { 
		session.execute("CREATE KEYSPACE IF NOT EXISTS simplex WITH replication " + 
			      "= {'class':'SimpleStrategy', 'replication_factor':3};");
		
		session.execute(
			      "CREATE TABLE IF NOT EXISTS simplex.songs (" +
			            "id uuid PRIMARY KEY," + 
			            "title text," + 
			            "album text," + 
			            "artist text," + 
			            "tags set<text>," + 
			            "data blob" + 
			            ");"
			       );
		
			session.execute(
			      "CREATE TABLE IF NOT EXISTS simplex.playlists (" +
			            "id uuid," +
			            "title text," +
			            "album text, " + 
			            "artist text," +
			            "song_id uuid," +
			            "PRIMARY KEY (id, title, album, artist)" +
			            ");"
			       );
	}
	
	public void loadData() { 
		session.execute(
			      "INSERT INTO simplex.songs (id, title, album, artist, tags) " +
			      "VALUES (" +
			          "756716f7-2e54-4715-9f00-91dcbea6cf50," +
			          "'La Petite Tonkinoise'," +
			          "'Bye Bye Blackbird'," +
			          "'Joséphine Baker'," +
			          "{'jazz', '2013'})" +
			          ";");
			session.execute(
			      "INSERT INTO simplex.playlists (id, song_id, title, album, artist) " +
			      "VALUES (" +
			          "2cc9ccb7-6221-4ccb-8387-f22b6a1b354d," +
			          "756716f7-2e54-4715-9f00-91dcbea6cf50," +
			          "'La Petite Tonkinoise'," +
			          "'Bye Bye Blackbird'," +
			          "'Joséphine Baker'" +
			          ");");
	}
	
	public void querySchema() {
		ResultSet results = session.execute("SELECT * FROM simplex.playlists " +
		        "WHERE id = 2cc9ccb7-6221-4ccb-8387-f22b6a1b354d;");
		
		System.out.println(String.format("%-30s\t%-20s\t%-20s\n%s", "title", "album", "artist",
			       "-------------------------------+-----------------------+--------------------"));
		for (Row row : results) {
			System.out.println(String.format("%-30s\t%-20s\t%-20s", row.getString("title"),
			row.getString("album"),  row.getString("artist")));
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
		SimpleClient2 client = new SimpleClient2();
		client.connect("127.0.0.1");
		client.createSchema();
		//client.loadData();
		client.querySchema();
		client.close();	
	}
	
	
}
