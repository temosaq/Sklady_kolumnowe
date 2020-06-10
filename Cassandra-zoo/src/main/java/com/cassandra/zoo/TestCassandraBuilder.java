package com.cassandra.zoo;

import com.datastax.oss.driver.api.core.CqlSession;

import java.util.Scanner;

public class TestCassandraBuilder {
	public static void main(String[] args) {
		try (CqlSession session = CqlSession.builder().build()) {
			KeyspaceBuilderManager keyspaceManager = new KeyspaceBuilderManager(session, "collection");
			keyspaceManager.dropKeyspace();
			keyspaceManager.selectKeyspaces();
			keyspaceManager.createKeyspace();
			keyspaceManager.useKeyspace();

			ZooTableBuilderManager tableManager = new ZooTableBuilderManager(session);


			do {
				Scanner scanner = new Scanner(System.in);
				System.out.println("");
				System.out.println("<--------------ZOO-------------->");
				System.out.println("-- 1. Stwórz tabelę");
				System.out.println("-- 2. Dodaj zwierzęta");
				System.out.println("-- 3. Wyświetl zoo po ID");
				System.out.println("-- 4. Wyświetl zoo z określoną powierzchnią");
				System.out.println("-- 5. Wyświetl wszyskie zoo");
				System.out.println("-- 6. Aktualizuj ");
				System.out.println("-- 7. Usuń wybrane zoo");
				System.out.println("-- 8. Usuń tabelę");

				System.out.println("Podaj numer operacji do wykonania: ");
				String option = scanner.nextLine();
				switch (option) {
					case "1":
						tableManager.createTable();
						break;

					case "2":
						tableManager.insertIntoTable();
						break;

					case "3":
						tableManager.selectById();
						break;

					case "4":
						tableManager.selectFromTable();
						break;

					case "5":
						tableManager.selectAll();
						break;

					case "6":
						tableManager.updateIntoTable();
						break;

					case "7":
						tableManager.deleteFromTable();
						break;

					case "8":
						tableManager.dropTable();
						break;

					default:
						System.out.println("Błędna instrukcja");
				}
			}while (true);

		}
	}
}
