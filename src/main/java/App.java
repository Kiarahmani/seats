import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Date;
import java.util.Properties;

import com.mysql.jdbc.Driver;
import com.mysql.jdbc.exceptions.jdbc4.MySQLTransactionRollbackException;

public class App {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String opType = args[0];
		int insID = Integer.valueOf(args[1]);
		Properties p = new Properties();
		p.setProperty("c_id", args[2]);
		Transaction txn = new Transaction(insID, p);
		txn.run(opType);

	}

}
