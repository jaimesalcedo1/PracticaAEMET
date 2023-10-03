package org.example.database;

import org.apache.ibatis.jdbc.ScriptRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Paths;
import java.sql.*;
import java.time.Instant;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.Properties;

public class DatabaseManager implements AutoCloseable{

    private static DatabaseManager instance;
    private final Logger logger = LoggerFactory.getLogger(DatabaseManager.class);
    private boolean databaseInitTables = false;
    private String serverPort;
    private String databaseName;
    private String jdbcDriver;
    private String connectionUrl;
    private String databaseUser;
    private String databasePassword;
    private String databaseInitScript = "init.sql";
    private Connection connection;

    private DatabaseManager(){

        loadProperties();

        try {
            openConnection();
            if (databaseInitTables){
                initTables();
            }
            closeConnection();
        } catch (SQLException e) {
            logger.error("Error al conectar con la base de datos" + e.getMessage());
        }
    }

    public static synchronized DatabaseManager getInstance() {
        if (instance == null) {
            instance = new DatabaseManager();
        }
        return instance;
    }

    public void loadProperties(){
        logger.debug("Cargando fichero de la configuración para la base de datos");
        try {
            var archivo = ClassLoader.getSystemResource("config.properties").getFile();
            var properties = new Properties();
            properties.load(new FileReader(archivo));

            serverPort = properties.getProperty("database.port", "3306");
            databaseName = properties.getProperty("database.name", "AppDataBase");
            jdbcDriver = properties.getProperty("database.driver", "org.h2.Driver");
            databaseInitTables = Boolean.parseBoolean(properties.getProperty("database.initTables", "false"));
            databaseInitScript = properties.getProperty("database.initScript", "init.sql");
            databaseUser = properties.getProperty("database.user", "sa");
            databasePassword = properties.getProperty("database.password", "");
            connectionUrl =
                    properties.getProperty("database.connectionUrl", "jdbc:h2:mem:" + databaseName + ";DB_CLOSE_DELAY=-1");

        } catch (IOException e){
            logger.debug("Error al leer el archivo de configuración");
        };

    }

    private void openConnection() throws SQLException {
        logger.debug("Conectando con la base de datos en " + connectionUrl);
        connection = DriverManager.getConnection(connectionUrl);

    }

    private void closeConnection()throws SQLException {
        logger.debug("Desconectando de la base de datos");
        connection.close();
    }

    private void initTables(){
        try {
            executeScript(databaseInitScript, true);
        } catch (FileNotFoundException e){
            logger.debug("Error inicializando la base de datos");

        }
    }

    public void executeScript(String scriptSQLFile, boolean logWriter) throws FileNotFoundException{
        ScriptRunner sr = new ScriptRunner(connection);
        var archivo = ClassLoader.getSystemResource(scriptSQLFile).getFile();
        logger.debug("Ejecutando el script " + archivo);
        Reader reader = new BufferedReader(new FileReader(archivo));
        sr.setLogWriter(logWriter ? new PrintWriter(System.out) : null);
        sr.runScript(reader);
    }

    public Connection getConnection() throws SQLException {
        if (connection == null || connection.isClosed()) {
            try {
                openConnection();
            } catch (SQLException e) {
                logger.error("Error al abrir la conexión con la base de datos " + e.getMessage());
                throw e;
            }
        }
        return connection;
    }

    @Override
    public void close() throws Exception {
        closeConnection();
    }
}
