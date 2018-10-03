package api;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map.Entry;

public class Printer {
	public void print(ResultSet results, List<Row> table) {
		try {
			Row.formTable(results, table);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		for (Row row : table) {
			for (Entry<Object, Class> col : row.row) {
				System.out.print(" | " + ((col.getValue()).cast(col.getKey())));
			}
			System.out.println();
		}
	}
}
