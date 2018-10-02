import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Date;
import java.util.Properties;

import org.omg.CORBA.COMM_FAILURE;

public class Seats {
	private Connection connect = null;
	private Statement statement = null;
	private PreparedStatement preparedStatement = null;
	private ResultSet resultSet = null;
	private int _ISOLATION = Connection.TRANSACTION_READ_UNCOMMITTED;
	private int insID;
	Properties p;

	public Seats(int insID) {
		this.insID = insID;
		p = new Properties();
		p.setProperty("ID", String.valueOf(insID));
		p.setProperty("dbName", "test");
	}

	// -----------------------------------------------------------
	public void initialize() throws Exception {

		try {

			Object o = Class.forName("MyDriver").newInstance();
			DriverManager.registerDriver((Driver) o);
			Driver driver = DriverManager.getDriver("jdbc:mydriver://");
			connect = driver.connect(null, p);
			connect.setAutoCommit(false);
			connect.setTransactionIsolation(_ISOLATION);

			//
			//
			//
			//
			//

			connect.commit();
		} catch (Exception e) {
			throw e;
		} finally {
			close();
		}

	}

	// -----------------------------------------------------------
	public void updateReservation() throws Exception {

		try {
			Object o = Class.forName("MyDriver").newInstance();
			DriverManager.registerDriver((Driver) o);
			Driver driver = DriverManager.getDriver("jdbc:mydriver://");
			connect = driver.connect(null, p);
			connect.setAutoCommit(false);
			connect.setTransactionIsolation(_ISOLATION);

			//
			//
			//
			//
			//

			connect.commit();
		} catch (Exception e) {
			throw e;
		} finally {
			close();
		}

	}

	// -----------------------------------------------------------
	public void findFlights() throws Exception {
		try {
			Object o = Class.forName("MyDriver").newInstance();
			DriverManager.registerDriver((Driver) o);
			Driver driver = DriverManager.getDriver("jdbc:mydriver://");
			connect = driver.connect(null, p);
			connect.setAutoCommit(false);
			connect.setTransactionIsolation(_ISOLATION);

			//
			//
			//
			//
			//

			connect.commit();
		} catch (Exception e) {
			throw e;
		} finally {
			close();
		}

	}

	// -----------------------------------------------------------
	public void findOpenSeats() throws Exception {
		try {
			Object o = Class.forName("MyDriver").newInstance();
			DriverManager.registerDriver((Driver) o);
			Driver driver = DriverManager.getDriver("jdbc:mydriver://");
			connect = driver.connect(null, p);
			connect.setAutoCommit(false);
			connect.setTransactionIsolation(_ISOLATION);

			//
			//
			//
			//

			connect.commit();
		} catch (Exception e) {
			throw e;
		} finally {
			close();
		}

	}

	// -----------------------------------------------------------
	public void newReservation() throws Exception {
		try {
			Object o = Class.forName("MyDriver").newInstance();
			DriverManager.registerDriver((Driver) o);
			Driver driver = DriverManager.getDriver("jdbc:mydriver://");
			connect = driver.connect(null, p);
			connect.setAutoCommit(false);
			connect.setTransactionIsolation(_ISOLATION);

			//
			//
			//
			//

			connect.commit();
		} catch (Exception e) {
			throw e;
		} finally {
			close();
		}

	}

	// -----------------------------------------------------------
	public void updateCustomer() {
		try {

			Object o = Class.forName("MyDriver").newInstance();
			DriverManager.registerDriver((Driver) o);
			Driver driver = DriverManager.getDriver("jdbc:mydriver://");
			connect = driver.connect(null, p);
			connect.setAutoCommit(false);
			connect.setTransactionIsolation(_ISOLATION);

			//
			//
			//
			//

			connect.commit();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			close();
		}
	}

	// -----------------------------------------------------------
	public void deleteReservation() throws Exception {
		try {
			Object o = Class.forName("MyDriver").newInstance();
			DriverManager.registerDriver((Driver) o);
			Driver driver = DriverManager.getDriver("jdbc:mydriver://");
			connect.setAutoCommit(false);
			connect = driver.connect(null, p);

			//
			//
			//
			//
			//

			connect.commit();
		} catch (Exception e) {
			throw e;
		} finally {
			close();
		}

	}

	private void close() {
		try {
			if (resultSet != null)
				resultSet.close();
			if (statement != null)
				statement.close();
			if (connect != null)
				connect.close();
		} catch (Exception e) {

		}
	}

}
