package my.first.step;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * @author JBD
 * 
 *         Cette classe vous aidera a faire vos premiers pas grâce à JDBC.
 *
 */
public class MyFirstStepWithJDBC {

	private Connection connection;
	private ResultSet resultSet;

	/**
	 * Cette méthode charge le pilote nécessaire à la connexion avec la BDD.
	 * Celui-ci prend la forme d'un jar (ici, il s'agit de
	 * mysql-connector-java-5.1.34-bin.jar, se trouvant à la racine du projet).
	 * 
	 * En cas d'exception, il est possible que le driver ne fasse pas partie du
	 * Classpath auquel cas, il est necessaire de le rajouter (sur Eclipse :
	 * clic droit sur le projet > Build path > Configure Build path > add jar >
	 * choisir le driver).
	 */
	public MyFirstStepWithJDBC() {
		try {
			Class.forName("com.mysql.jdbc.Driver");
		} catch (ClassNotFoundException e) {
			System.out.println("Impossible de charger le pilote");
			throw new RuntimeException(e);
		}
	}

	/**
	 * Cette méthode permet de lancer une connection avec une BDD. Dans le cas
	 * où la BDD n'aurait pas besoin d'identifiant et/ou de mot de passe, il
	 * suffit de laisser ces champs à null.
	 * 
	 * Dans le cadre de ce tutoriel, on utilisera root pour se connecter à la
	 * BDD néanmoins, dans le cadre d'un véritable projet, il est fortement
	 * conseillé d'utiliser un utilisateur créé pour l'occasion afin qu'il
	 * bénéficie de droit dédiés (et donc pas des droits root). Après tout, il
	 * est peu probable qu'un utilisateur ait le droit de supprimer des tables !
	 * 
	 * @param bddAdress
	 * @param login
	 * @param password
	 */
	public void startConnection(final String bddAdress, final String login, final String password) {
		try {
			connection = DriverManager.getConnection(bddAdress, login, password);
		} catch (SQLException e) {
			System.out.println("Problème de connexion !");
			throw new RuntimeException(e);
		}
	}

	/**
	 * Cette méthode permet de créer la table client, produit et commande dont
	 * on a besoin pour effectuer nos premiers pas avec JDBC.
	 * 
	 * Dans le cadre de véritable projet, il est fortement déconseillé de
	 * conserver la structure des tables dans un programme java, on préfèreras
	 * utiliser un fichier .sql qu'il faudrat executer pour créer directement la
	 * structure de la base de données.
	 */
	public void createTable() {
		try {
			String requeteClient = "CREATE TABLE IF NOT EXISTS Client(idClient INT, nom VARCHAR(100), prenom VARCHAR(100));";
			String requeteProduit = "CREATE TABLE IF NOT EXISTS Produit(idProduit INT, libelle VARCHAR(100), prix FLOAT);";
			String requeteCommande = "CREATE TABLE IF NOT EXISTS Commande(idClient INT, idProduit INT, dateCommande TIMESTAMP);";
			/*
			 * On utilise ici la méthode executeUpdate() car la requête n'est
			 * pas censée renvoyer de données.
			 */
			Statement stmt = connection.createStatement();
			stmt.executeUpdate(requeteClient);
			stmt.executeUpdate(requeteProduit);
			stmt.executeUpdate(requeteCommande);
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Cette méthode permet d'inserer en base de données quelques
	 * enregistrements dans le but de tester nos futures requêtes.
	 */
	public void insertClient() {
		try {
			Integer idClient1 = 1;
			String nomClient1 = "nomClient1";
			String prenomClient1 = "prenomClient1";
			/*
			 * Pour créer une requête paramétrée, il existe deux manières de
			 * faire avec JDBC. La première, la mauvaise, consiste simplement à
			 * concatener les différentes données. On pourra executer la requête
			 * grâce à un Statement comme vu précédemment.
			 */
			String requeteClient1 = "INSERT INTO Client value (" + idClient1 + ", '" + nomClient1
					+ "', '" + prenomClient1 + "');";
			Statement stmt = connection.createStatement();
			stmt.executeUpdate(requeteClient1);

			Integer idClient2 = 2;
			String nomClient2 = "nomClient2";
			String prenomClient2 = "prenomClient2";

			/*
			 * La seconde, bien plus propre consiste à indiquer où devront être
			 * placées les différents paramètres puis à les insérer selon leur
			 * position. Cette manière de faire permet notamment de se prémunir
			 * de l'injection de sql.
			 */
			String requeteClient2 = "INSERT INTO Client value (?, ?, ?);";
			PreparedStatement pstmt = connection.prepareStatement(requeteClient2);
			pstmt.setInt(1, idClient2);
			pstmt.setString(2, nomClient2);
			pstmt.setString(3, prenomClient2);
			pstmt.executeUpdate();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Cette methode permet d'afficher toutes les tables présentes en base de
	 * données ainsi que, pour chacune, les colonnes qui la compose
	 */
	public void printDatabase() {
		try {
			DatabaseMetaData dmd = connection.getMetaData();
			ResultSet rs = dmd.getTables(null, null, null, null);
			System.out.println("Liste des tables existantes : ");
			while (rs.next()) {
				/*
				 * Dans le DatabaseMetaData, l'indice 3 correspond au nom des
				 * tables
				 */
				System.out.println("Table " + rs.getString(3) + " : ");
				/*
				 * A partir du nom de la table, on effectue une requête select
				 * afin de récuperer le ResultSetMetaData. Ici, on utilisera
				 * tout de même un simple Statement car il n'est pas possible
				 * d'inserer dynamiquement le nom de la table.
				 */
				Statement s = connection.createStatement();
				ResultSet rs2 = s.executeQuery("select * from " + rs.getString(3) + ";");
				ResultSetMetaData rsmd = rs2.getMetaData();
				for (int i = 1; i <= rsmd.getColumnCount(); i++) {
					System.out.println(rsmd.getColumnLabel(i));
				}
				System.out.println();
			}
		} catch (SQLException e) {
			System.out.println("Anomalie lors de l'execution de la requête");
			throw new RuntimeException(e);
		}
	}

	/**
	 * Cette méthode permet de supprimer les tables Commande, Produit et Client.
	 * Il est fortement déconseillé d'implémenter une telle méthode en java. En
	 * effet, dans le cadre d'un vrai projet une fois la structure créée, on
	 * préfèrera créer un nouveau fichier .sql qui permettra d'effectuer les
	 * changements.
	 */
	public void dropTable() {
		try {
			String requeteCommande = "DROP TABLE IF EXISTS Client;";
			String requeteClient = "DROP TABLE IF EXISTS Client;";
			String requeteProduit = "DROP TABLE IF EXISTS Client;";

			Statement stmt = connection.createStatement();
			stmt.executeUpdate(requeteCommande);
			stmt.executeUpdate(requeteClient);
			stmt.executeUpdate(requeteProduit);
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Cette méthode permet d'afficher tous les clients existant en BDD.
	 */
	public void getClient() {
		String requete = "SELECT * FROM Client";
		/*
		 * Ici, le statement doit renvoyer des données, on utilise donc la
		 * méthode executeQuery() et on stockera les données renvoyées dans un
		 * ResultSet
		 */
		try {
			Statement stmt = connection.createStatement();
			resultSet = stmt.executeQuery(requete);

			System.out.println("parcours des données retournées");
			/*
			 * ResultSetMetaData contient toutes les données liés au ResultSet
			 * notamment : le nombre de colonne retournée, le nom d'une colonne
			 * en particulier, le type de donnée (au sens SQL), etc.
			 */
			ResultSetMetaData rsmd = resultSet.getMetaData();
			int nbCols = rsmd.getColumnCount();
			/*
			 * la méthode next() permet d'avancer le curseur se trouvant sur le
			 * résultat de la requête d'une ligne. Le premier appel de next()
			 * place le curseur sur la première ligne. Si la dernière ligne est
			 * atteinte, next() renvoie false.
			 */
			while (resultSet.next()) {
				for (int i = 1; i <= nbCols; i++) {
					System.out.print(rsmd.getColumnLabel(i) + " : " + resultSet.getString(i) + " ");
				}
				System.out.println();
			}
			resultSet.close();
		} catch (SQLException e) {
			System.out.println("Anomalie lors de l'execution de la requête");
			throw new RuntimeException(e);
		}
	}

	/**
	 * Grâce à cette méthode, on tente de fermer la connection, peu importe
	 * comment le programme se déroule. Trop de connexion non fermées peuvent
	 * encombrer la BDD, de plus, fermer une connexion permet de s'assurer que
	 * la transaction est bien terminée.
	 */
	public void disconnect() {

		try {
			if (connection != null) {
				connection.close();
			}
		} catch (SQLException e) {
			System.out.println(e.getMessage());
			System.exit(-1);
		}
	}

	public static void main(String[] args) {
		MyFirstStepWithJDBC JDBC = new MyFirstStepWithJDBC();
		JDBC.startConnection("jdbc:mysql://localhost/jdbc", "root", "root");
		/*
		 * Pour cet exercice, on créera et supprimera les différentes tables à
		 * chaque execution. Dans le cadre d'un véritable projet, il est évident
		 * qu'il ne faut effectuer qu'une seule fois la création des tables et
		 * n'executer la suppression qu'en cas de besoin.
		 */
		JDBC.createTable();
		JDBC.insertClient();

		JDBC.printDatabase();
		JDBC.getClient();

		JDBC.dropTable();
		JDBC.disconnect();
	}
}