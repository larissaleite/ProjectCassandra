package br.com.cassandra.dao;

import java.util.Iterator;
import java.util.List;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.eaio.uuid.UUID;

public class EstoqueDao implements IEstoqueDao {

	public void salvarProduto(String nome, float preco, List<String> tags) {
		
		UUID id_produto = new UUID();
		
		String tagString;
		
		if (tags.isEmpty()) {
			tagString = "{}";
		} else {
		 
		tagString = "{";
		
		for (String tag : tags) {
			tagString = tagString+"'"+tag+"',";
		}
		
		tagString = tagString.substring(0, tagString.length()-1);
		tagString = tagString +"}";
		System.out.println("TagString = "+tagString);
		}
		
		
		String query = "INSERT INTO estoque.produtos (id, nome, preco, tags) " +
			      "VALUES (" +
		          ""+id_produto+"," +
		          "'"+nome+"'," +
		          ""+preco+"," +
		          ""+tagString+")" +
		          ";";
		
		Configuration.getSession().execute(query);
		
	}

	public void salvarRecebimento(int quantidade, int mes, String nomeProduto) {
		
		UUID id_recebimento = new UUID();
		
		/*you can only query by columns that are part of the key*/
		ResultSet results = Configuration.getSession().execute("SELECT * FROM estoque.produtos WHERE nome='"+nomeProduto+"';");
	
		//java.util.UUID id_produto =  results.all().get(0).getUUID("id");
		Iterator<Row> iterator = results.iterator();
		Row row = iterator.next();
		java.util.UUID id_produto = row.getUUID("id");
		
		String query = "INSERT INTO estoque.recebimentos (id, produto_id, quantidade, mes) " +
			      "VALUES (" +
		          ""+id_recebimento+"," +
		          ""+id_produto+"," +
		          ""+quantidade+"," +
		          ""+mes+"" +
		          ");";
		
		Configuration.getSession().execute(query);
		
	}

	public void salvarVenda(int quantidade, int mes, String nomeProduto) {
		
		UUID id_venda = new UUID();
		
		ResultSet results = Configuration.getSession().execute("SELECT id FROM estoque.produtos WHERE nome='"+nomeProduto+"' allow filtering;");
		java.util.UUID id_produto = results.all().get(0).getUUID("id");
		
		String query = "INSERT INTO estoque.vendas (id, produto_id, quantidade, mes) " +
			      "VALUES (" +
		          ""+id_venda+"," +
		          ""+id_produto+"," +
		          ""+quantidade+"," +
		          ""+mes+"" +
		          ");";
		
		Configuration.getSession().execute(query);
		
	}

	public void recuperarProdutos() {
		ResultSet results = Configuration.getSession().execute("SELECT * FROM estoque.produtos;");

		System.out.println(String.format("%-40s\t%-20s\t%-20s\n%s", "id", "nome", "preco",
			       "-----------------------------------+-----------------------+--------------------"));
		for (Row row : results) {
			System.out.println(String.format("%-40s\t%-20s\t%-20s", row.getUUID("id"),
			row.getString("nome"),  row.getFloat("preco")));
		}
		
	}

	public void recuperarRecebimentos() {
		ResultSet results = Configuration.getSession().execute("SELECT * FROM estoque.recebimentos;");

		System.out.println(String.format("%-40s\t%-20s\t%-20s\n%s", "produto_id", "quantidade", "mes",
			       "-----------------------------------+-----------------------+--------------------"));
		for (Row row : results) {
			System.out.println(String.format("%-40s\t%-20s\t%-20s", row.getUUID("produto_id"),
			row.getInt("quantidade"),  row.getInt("mes")));
		}
		
		
	}
	
	

}
