package br.com.cassandra.main;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

import br.com.cassandra.dao.Configuration;
import br.com.cassandra.dao.EstoqueDao;
import br.com.cassandra.dao.IEstoqueDao;

public class Main {

	public static void main(String[] args) {
		IEstoqueDao estoqueDao = new EstoqueDao();
		
	 	List<String> tags1 = new ArrayList<String>();
		tags1.add("escritorio");
		tags1.add("eletronicos");
		tags1.add("computador");
		estoqueDao.salvarProduto("Impressora", 450, tags1);
		
		List<String> tags2 = new ArrayList<String>();
		tags2.add("escritorio");
		tags2.add("eletronicos");
		estoqueDao.salvarProduto("Pen Drive", 50, tags2);
		
		List<String> tags3 = new ArrayList<String>();
		tags3.add("cozinha");
		tags3.add("escritorio");
		estoqueDao.salvarProduto("Caneca", 15, tags3);
		
		estoqueDao.salvarRecebimento(50, 4, "Impressora");
		
		estoqueDao.salvarRecebimento(400, 8, "Pen Drive");
		
		/*estoqueDao.salvarProduto(80, 50, new ArrayList<String>());
		estoqueDao.salvarRecebimento(5, 4, 80);*/
		
		
		estoqueDao.recuperarProdutos();
		System.out.println();
		estoqueDao.recuperarRecebimentos();
		Configuration.close();
	}

}
