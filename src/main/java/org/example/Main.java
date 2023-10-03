package org.example;

import org.example.database.DatabaseManager;
import org.example.repository.aemet.AemetRepositoryImpl;

public class Main {
    public static void main(String[] args) {
        var dbm = DatabaseManager.getInstance();
        var aemetRepoImpl = AemetRepositoryImpl.getInstance(dbm);
        aemetRepoImpl.llenarTablas();
    }
}