package br.com.cassandra.dao;

import java.util.List;

public interface IEstoqueDao {

	public void salvarProduto(String nome, float preco, List<String> tags);
	public void salvarRecebimento(int quantidade, int mes, String nomeProduto);
	public void salvarVenda(int quantidade, int mes, String nomeProduto);
	
	/* change return type later */
	public void recuperarProdutos();
	public void recuperarRecebimentos();

}
