package org.example.repository.crud;

import java.sql.SQLException;
import java.util.List;
import java.util.Optional;

public interface CrudRepository<T, ID, EX extends Exception> {

    // Guardar
    T save(T t) throws SQLException, EX;

    // Actualizar
    T update(T t) throws SQLException, EX;

    // Buscar por ID
    Optional<T> findById(ID id) throws SQLException;

    // Buscar todos
    List<T> findAll() throws SQLException;

    // Borrar por ID
    boolean deleteById(ID id) throws SQLException;

    // Borrar todos
    void deleteAll() throws SQLException;
}

