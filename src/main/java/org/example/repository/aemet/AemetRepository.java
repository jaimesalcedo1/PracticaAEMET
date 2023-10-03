package org.example.repository.aemet;

import org.example.exceptions.AemetException;
import org.example.models.Aemet;
import org.example.repository.crud.CrudRepository;

import java.sql.SQLException;
import java.util.List;

public interface AemetRepository extends CrudRepository<Aemet, Long, AemetException> {

    List<Aemet> findByLocalidad(String localidad) throws SQLException;
}
