package org.example;

import org.example.controllers.AemetController;
import org.example.database.DatabaseManager;
import org.example.repository.aemet.AemetRepositoryImpl;

import java.sql.SQLException;

public class Main {
    public static void main(String[] args) throws SQLException {
        var aemetController = AemetController.getInstance();
    }
}