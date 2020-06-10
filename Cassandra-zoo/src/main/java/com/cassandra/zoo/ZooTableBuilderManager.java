package com.cassandra.zoo;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import com.datastax.oss.driver.api.querybuilder.delete.Delete;
import com.datastax.oss.driver.api.querybuilder.insert.Insert;
import com.datastax.oss.driver.api.querybuilder.schema.CreateTable;
import com.datastax.oss.driver.api.querybuilder.schema.CreateType;
import com.datastax.oss.driver.api.querybuilder.schema.Drop;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import com.datastax.oss.driver.api.querybuilder.update.Update;


import java.util.Scanner;

public class ZooTableBuilderManager extends SimpleManager {

	public ZooTableBuilderManager(CqlSession session) {
		super(session);
	}

	Scanner scanner = new Scanner(System.in);

	public void createTable() {
		CreateType createType = SchemaBuilder.createType("address").withField("street", DataTypes.TEXT)
				.withField("houseNumber", DataTypes.INT).withField("apartmentNumber", DataTypes.INT);

		session.execute(createType.build());

		CreateTable createTable = SchemaBuilder.createTable("zoo")
				.withPartitionKey("id", DataTypes.INT)
				.withColumn("name", DataTypes.TEXT)
//				.withColumn("name", DataTypes.listOf(DataTypes.TEXT))
				.withColumn("address", QueryBuilder.udt("address"))
				.withColumn("animals", DataTypes.listOf(DataTypes.TEXT))
				.withColumn("area", DataTypes.INT)
				.withColumn("tourists", DataTypes.INT)
				.withColumn("tickets", DataTypes.mapOf(DataTypes.TEXT, DataTypes.DOUBLE));
		session.execute(createTable.build());
	}

	public void insertIntoTable() {
		Insert insert = QueryBuilder.insertInto("collection", "zoo")
				.value("id", QueryBuilder.raw("1"))
				.value("name", QueryBuilder.raw("'Zoo Wrocław'"))
				.value("address", QueryBuilder.raw("{street : 'Jaracza', houseNumber : 5, apartmentNumber : 9}"))
				.value("animals", QueryBuilder.raw("['lew', 'tygrys', 'żyrafa', 'zebra']"))
				.value("area", QueryBuilder.raw("33"))
				.value("tourists", QueryBuilder.raw("800"))
				.value("tickets", QueryBuilder.raw("{'normal':15.0, 'reduced':10.0}"));
		session.execute(insert.build());

		Insert insert2 = QueryBuilder.insertInto("collection", "zoo")
				.value("id", QueryBuilder.raw("2"))
				.value("name", QueryBuilder.raw("'Zoo Łodź'"))
				.value("address", QueryBuilder.raw("{street : 'Wróbelskiego', houseNumber : 1, apartmentNumber : 5}"))
				.value("animals", QueryBuilder.raw("['krokodyl', 'foka', 'delfin']"))
				.value("area", QueryBuilder.raw("17"))
				.value("tourists", QueryBuilder.raw("1000"))
				.value("tickets", QueryBuilder.raw("{'normal':17.0, 'reduced':11.0}"));
		session.execute(insert2.build());

		Insert insert3 = QueryBuilder.insertInto("collection", "zoo")
				.value("id", QueryBuilder.raw("3"))
				.value("name", QueryBuilder.raw("'Zoo Poznań'"))
				.value("address", QueryBuilder.raw("{street : 'Kolejowa', houseNumber : 2, apartmentNumber : 33}"))
				.value("animals", QueryBuilder.raw("['żyrafa', 'zebra', 'małpa', 'szympans']"))
				.value("area", QueryBuilder.raw("12"))
				.value("tourists", QueryBuilder.raw("500"))
				.value("tickets", QueryBuilder.raw("{'normal':13.0, 'reduced':9.0}"));
		session.execute(insert3.build());
	}

	public void updateIntoTable() {
		System.out.println("Podaj id zoo do aktualizacji");
		int idUp = scanner.nextInt();
		scanner.nextLine();
		System.out.println("Podaj nową powierzchnię");
		int newArea = scanner.nextInt();
		Update update = QueryBuilder.update("zoo").setColumn("area", QueryBuilder.literal(newArea)).whereColumn("id").isEqualTo(QueryBuilder.literal(idUp));
		session.execute(update.build());
	}


	public void deleteFromTable() {
		System.out.println("Podaj id zoo do usunięcia");
		int idDel = scanner.nextInt();
		Delete delete = QueryBuilder.deleteFrom("zoo").whereColumn("id").isEqualTo(QueryBuilder.literal(idDel));
		session.execute(delete.build());
	}

	public void selectAll() {

		Select query = QueryBuilder.selectFrom("zoo").all();
		SimpleStatement statement = query.build();

		ResultSet resultSet = session.execute(statement);
		for (Row row : resultSet) {
			System.out.print("zoo: ");
			System.out.print(row.getInt("id") + ", ");
			System.out.print(row.getString("name") + ", ");
			UdtValue address = row.getUdtValue("address");
			System.out.print(row.getList("animals", String.class) + ", " );
			System.out.print(row.getInt("area") + ", ");
			System.out.print("{" + address.getString("street") + ", " + address.getInt("houseNumber") + ", "
					+ address.getInt("apartmentNumber") + "}" + ", ");
			System.out.print(row.getInt("tourists") + ", ");
			System.out.print(row.getMap("tickets", String.class, Double.class) + ", ");
			System.out.println();
		}
		System.out.println("Statement \"" + statement.getQuery() + "\" executed successfully");
	}

	public void selectFromTable() {

		System.out.println("Wyświetl zoo z powierzchnią większą od:");
		Select query = QueryBuilder.selectFrom("zoo").all().allowFiltering().whereColumn("area").isGreaterThan(QueryBuilder.literal(scanner.nextInt()));

		SimpleStatement statement = query.build();

		ResultSet resultSet = session.execute(statement);
		for (Row row : resultSet) {
			System.out.print("zoo: ");
			System.out.print(row.getInt("id") + ", ");
			System.out.print(row.getString("name") + ", ");
			UdtValue address = row.getUdtValue("address");
			System.out.print(row.getList("animals", String.class) + ", " );
			System.out.print(row.getInt("area") + ", ");
			System.out.print("{" + address.getString("street") + ", " + address.getInt("houseNumber") + ", "
					+ address.getInt("apartmentNumber") + "}" + ", ");
			System.out.print(row.getInt("tourists") + ", ");
			System.out.print(row.getMap("tickets", String.class, Double.class) + ", ");
			System.out.println();
		}
		System.out.println("Statement \"" + statement.getQuery() + "\" executed successfully");
	}


	public void selectById() {

		System.out.println("Podaj id zoo, które chcesz wyświetlić:");
		Select query = QueryBuilder.selectFrom("zoo").all().allowFiltering().whereColumn("id").isEqualTo(QueryBuilder.literal(scanner.nextInt()));

		SimpleStatement statement = query.build();

		ResultSet resultSet = session.execute(statement);
		for (Row row : resultSet) {
			System.out.print("zoo: ");
			System.out.print(row.getInt("id") + ", ");
			System.out.print(row.getString("name") + ", ");
			UdtValue address = row.getUdtValue("address");
			System.out.print(row.getList("animals", String.class) + ", " );
			System.out.print(row.getInt("area") + ", ");
			System.out.print("{" + address.getString("street") + ", " + address.getInt("houseNumber") + ", "
					+ address.getInt("apartmentNumber") + "}" + ", ");
			System.out.print(row.getInt("tourists") + ", ");
			System.out.print(row.getMap("tickets", String.class, Double.class) + ", ");
			System.out.println();
		}
		System.out.println("Statement \"" + statement.getQuery() + "\" executed successfully");

		System.out.println("");
	}

	public void dropTable() {
		Drop drop = SchemaBuilder.dropTable("zoo");
		executeSimpleStatement(drop.build());
	}
}
