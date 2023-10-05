package org.example.repository.aemet;

import org.example.database.DatabaseManager;
import org.example.exceptions.AemetException;
import org.example.exceptions.AemetNoEncotradoException;
import org.example.models.Aemet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class AemetRepositoryImpl implements AemetRepository{

    private static AemetRepositoryImpl instance;
    private final Logger logger = LoggerFactory.getLogger(AemetRepositoryImpl.class);
    private final DatabaseManager dbm;

    public AemetRepositoryImpl(DatabaseManager dbm) {
        this.dbm = dbm;
    }

    public static AemetRepositoryImpl getInstance(DatabaseManager dbm){
        if(instance == null){
            instance = new AemetRepositoryImpl(dbm);
        }
        return instance;
    }

    @Override
    public List<Aemet> findByLocalidad(String localidad) throws SQLException {
        logger.debug("Datos del pokemon: " + localidad);
        String query = "SELECT * FROM MEDICIONES WHERE localidad LIKE ?";
        try (var connection = dbm.getConnection();
             var ptsm = connection.prepareStatement(query)
        ) {
            ptsm.setString(1, "%" + localidad + "%");
            var rs = ptsm.executeQuery();
            var lista = new ArrayList<Aemet>();
            while (rs.next()) {

                Aemet aemet = Aemet.builder()
                        .dia(rs.getInt("dia"))
                        .localidad(rs.getString("localidad"))
                        .provincia(rs.getString("provincia"))
                        .tempMaxima(rs.getDouble("tempMaxima"))
                        .horaTempMax(LocalTime.parse(rs.getString("horaTempMax"), DateTimeFormatter.ofPattern("H:m")))
                        .tempMinima(rs.getDouble("tempMinima"))
                        .horaTempMin(LocalTime.parse(rs.getString("horaTempMin"), DateTimeFormatter.ofPattern("H:m")))
                        .precipitacion(rs.getDouble("precipitacion"))
                        .build();

                lista.add(aemet);
            }
            return lista;
        }
    }

    @Override
    public Aemet save(Aemet aemet) throws SQLException, AemetException {
        logger.debug("Guardando el funko: " + aemet);
        String query = "INSERT INTO MEDICIONES(cod, dia, localidad, provincia, tempMaxima, horaTempMax, tempMinima, horaTempMin, precipitacion) VALUES (?, ?, ?, ?, ?, ?, ?, ?)";

        try (var connection = dbm.getConnection();
             var pstm = connection.prepareStatement(query, Statement.RETURN_GENERATED_KEYS)
        ) {
            pstm.setInt(1, aemet.getCod());
            pstm.setInt(2, aemet.getDia());
            pstm.setString(3, aemet.getLocalidad());
            pstm.setString(4, aemet.getProvincia());
            pstm.setDouble(5, aemet.getTempMaxima());
            pstm.setString(6, String.valueOf(aemet.getHoraTempMax()));
            pstm.setDouble(7, aemet.getTempMinima());
            pstm.setString(8, String.valueOf(aemet.getHoraTempMin()));
            pstm.setDouble(9, aemet.getPrecipitacion());
        }
        return aemet;
    }

    @Override
    public Aemet update(Aemet aemet) throws SQLException, AemetException {
        logger.debug("Actualizando la medición: " + aemet);
        String query = "UPDATE MEDICIONES SET localidad =?, provincia =?, tempMaxima =?, horaTempMax =?, tempMinima =?, horaTempMin =?, precipitacion =? WHERE cod =?";
        try (var connection = dbm.getConnection();
             var pstm = connection.prepareStatement(query)
        ) {
            pstm.setString(1, aemet.getLocalidad());
            pstm.setString(2, aemet.getProvincia());
            pstm.setDouble(3, aemet.getTempMaxima());
            pstm.setString(4, String.valueOf(aemet.getHoraTempMax()));
            pstm.setDouble(5, aemet.getTempMinima());
            pstm.setString(6, String.valueOf(aemet.getHoraTempMin()));
            pstm.setDouble(7, aemet.getPrecipitacion());
            pstm.setInt(8, aemet.getCod());

            var res = pstm.executeUpdate();
            if (res > 0) {
                logger.debug("medición actualizada");
            } else {
                logger.error("medición no actualizada al no encontrarse en la base de datos con id" + aemet.getCod());
                throw new AemetNoEncotradoException("medición no encontrada");
            }
        }
        return aemet;
    }

    @Override
    public Optional<Aemet> findById(Long id) throws SQLException {
        logger.debug("Obteniendo el funko con id: " + id);
        String query = "SELECT * FROM MEDICIONES WHERE ID = ?";
        try (var connection = dbm.getConnection();
             var stmt = connection.prepareStatement(query)
        ) {
            stmt.setLong(1, id);
            var rs = stmt.executeQuery();
            Optional<Aemet> aemet = Optional.empty();
            while (rs.next()) {

                aemet = Optional.of(Aemet.builder()
                        .cod(rs.getInt("cod"))
                        .localidad(rs.getString("localidad"))
                        .provincia(rs.getString("provincia"))
                        .tempMaxima(rs.getDouble("tempMaxima"))
                        .horaTempMax(LocalTime.parse(rs.getString("horaTempMax")))
                        .tempMinima(rs.getDouble("tempMinima"))
                        .horaTempMin(LocalTime.parse(rs.getString("horaTempMin")))
                        .precipitacion(rs.getDouble("precipitacion"))
                        .build()
                );
            }
            return aemet;
        }
    }

    @Override
    public List<Aemet> findAll() throws SQLException {
        logger.debug("Obteniendo todos las mediciones");
        var query = "SELECT * FROM MEDICIONES";
        try (var connection = dbm.getConnection();
             var pstm = connection.prepareStatement(query)
        ) {
            var rs = pstm.executeQuery();
            var lista = new ArrayList<Aemet>();
            while (rs.next()) {
                Aemet aemet = Aemet.builder()
                        .cod(rs.getInt("cod"))
                        .dia(rs.getInt("dia"))
                        .localidad(rs.getString("localidad"))
                        .provincia(rs.getString("provincia"))
                        .tempMaxima(rs.getDouble("tempMaxima"))
                        .horaTempMax(LocalTime.parse(rs.getString("horaTempMax"), DateTimeFormatter.ofPattern("H:m")))
                        .tempMinima(rs.getDouble("tempMinima"))
                        .horaTempMin(LocalTime.parse(rs.getString("horaTempMin"), DateTimeFormatter.ofPattern("H:m")))
                        .precipitacion(rs.getDouble("precipitacion"))
                        .build();
                lista.add(aemet);
            }
            return lista;
        }
    }

    @Override
    public boolean deleteById(Long id) throws SQLException {
        logger.debug("Borrando la medición con id: " + id);
        String query = "DELETE FROM MEDICIONES WHERE ID =?";
        try (var connection = dbm.getConnection();
             var pstm = connection.prepareStatement(query)
        ) {
            pstm.setLong(1, id);
            var res = pstm.executeUpdate();
            return res > 0;
        }
    }

    @Override
    public void deleteAll() throws SQLException {
        logger.debug("Borrando todos las mediciones");
        String query = "DELETE FROM MEDICIONES";
        try (var connection = dbm.getConnection();
             var pstm = connection.prepareStatement(query)
        ) {
            pstm.executeUpdate();
        }
    }

    public void llenarTablas(){
        logger.debug("Guardando datos en la base de datos");
        String insertAllQuery = "INSERT INTO MEDICIONES(cod, dia, localidad, provincia, tempMaxima, horaTempMax, tempMinima, horaTempMin, precipitacion) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";

        try(var connection = dbm.getConnection();
            var pstm = connection.prepareStatement(insertAllQuery, Statement.RETURN_GENERATED_KEYS)
        ) {
            String csv1 = Paths.get("").toAbsolutePath() + File.separator + "data" + File.separator + "Aemet20171029.csv";
            String csv2 = Paths.get("").toAbsolutePath() + File.separator + "data" + File.separator + "Aemet20171030.csv";
            String csv3 = Paths.get("").toAbsolutePath() + File.separator + "data" + File.separator + "Aemet20171031.csv";
            BufferedReader br1 = new BufferedReader(new FileReader(csv1));
            BufferedReader br2 = new BufferedReader(new FileReader(csv2));
            BufferedReader br3 = new BufferedReader(new FileReader(csv3));
            String line;
            var contadorCOD = 0;

            while((line = br1.readLine()) != null){
                String[] data = line.split(";");
                contadorCOD++;
                pstm.setInt(1, contadorCOD);
                pstm.setInt(2, 29);
                pstm.setString(3, data[0]);
                pstm.setString(4, data[1]);
                pstm.setDouble(5, Double.parseDouble(data[2]));
                pstm.setString(6, data[3]);
                pstm.setDouble(7, Double.parseDouble(data[4]));
                pstm.setString(8, data[5]);
                pstm.setDouble(9, Double.parseDouble(data[6]));
                pstm.addBatch();
            }
            while((line = br2.readLine()) != null){
                String[] data = line.split(";");
                contadorCOD++;
                pstm.setInt(1, contadorCOD);
                pstm.setInt(2, 30);
                pstm.setString(3, data[0]);
                pstm.setString(4, data[1]);
                pstm.setDouble(5, Double.parseDouble(data[2]));
                pstm.setString(6, data[3]);
                pstm.setDouble(7, Double.parseDouble(data[4]));
                pstm.setString(8, data[5]);
                pstm.setDouble(9, Double.parseDouble(data[6]));
                pstm.addBatch();
            }
            while((line = br3.readLine()) != null){
                String[] data = line.split(";");
                contadorCOD++;
                pstm.setInt(1, contadorCOD);
                pstm.setInt(2, 31);
                pstm.setString(3, data[0]);
                pstm.setString(4, data[1]);
                pstm.setDouble(5, Double.parseDouble(data[2]));
                pstm.setString(6, data[3]);
                pstm.setDouble(7, Double.parseDouble(data[4]));
                pstm.setString(8, data[5]);
                pstm.setDouble(9, Double.parseDouble(data[6]));
                pstm.addBatch();
            }
            pstm.executeBatch();
            connection.close();

        } catch (SQLException | IOException e) {
            throw new RuntimeException(e);
        }
        System.out.println("Base de datos cargada");
    }
}
