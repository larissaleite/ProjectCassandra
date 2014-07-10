package br.com.cassandra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;

public class SimpleClient {
	private Cluster cluster;

	public void connect(String node) {
		/*Add contact point and builds a cluster instance*/
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
	}

	public void close() {
		cluster.close();	
	}

		   
	public static void main(String[] args) {
		SimpleClient client = new SimpleClient();
		client.connect("127.0.0.1");
		client.close();	
	}
}
