package org.example.controllers;

import org.example.database.DatabaseManager;
import org.example.models.Aemet;
import org.example.repository.aemet.AemetRepositoryImpl;

import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

import static java.util.Comparator.comparing;
import static java.util.Comparator.comparingDouble;
import static java.util.stream.Collectors.*;

public class AemetController {

    private static AemetController instance;
    private List<Aemet> listaAemet;

    private AemetController() throws SQLException {
        var dbm = DatabaseManager.getInstance();
        var aemetRepoImpl = AemetRepositoryImpl.getInstance(dbm);
        aemetRepoImpl.llenarTablas();
        listaAemet = aemetRepoImpl.findAll();
        procesarStreams();
        generateJSON();
    }

    public static AemetController getInstance() throws SQLException {
        if(instance == null){
            instance = new AemetController();
        }
        return instance;
    }

    public void procesarStreams(){
        System.out.println("\n---DONDE SE DIO LA TEMPERATURA MAXIMA Y MINIMA PARA CADA UNO DE LOS DIAS ---");

        System.out.println("\n--- DIA 29 ---");
        var max29 = listaAemet.stream()
                .filter(x -> x.getDia() == 29)
                .max(comparingDouble(Aemet::getTempMaxima))
                .map(Aemet::getLocalidad).orElse("");
        var min29 = listaAemet.stream()
                .filter(x -> x.getDia() == 29)
                .min(comparingDouble(Aemet::getTempMinima))
                .map(Aemet::getLocalidad).orElse("");
        System.out.println("Temperatura maxima: " + max29 + "\nTemperatura minima: " + min29);

        System.out.println("\n--- DIA 30 ---");
        var max30 = listaAemet.stream()
                .filter(x -> x.getDia() == 30)
                .max(comparingDouble(Aemet::getTempMaxima))
                .map(Aemet::getLocalidad).orElse("");
        var min30 = listaAemet.stream()
                .filter(x -> x.getDia() == 30)
                .min(comparingDouble(Aemet::getTempMinima))
                .map(Aemet::getLocalidad).orElse("");
        System.out.println("Temperatura maxima: " + max30 + "\nTemperatura minima: " + min30);

        System.out.println("\n--- DIA 31 ---");
        var max31 = listaAemet.stream()
                .filter(x -> x.getDia() == 30)
                .max(comparingDouble(Aemet::getTempMaxima))
                .map(Aemet::getLocalidad).orElse("");
        var min31 = listaAemet.stream()
                .filter(x -> x.getDia() == 31)
                .min(comparingDouble(Aemet::getTempMinima))
                .map(Aemet::getLocalidad).orElse("");
        System.out.println("Temperatura maxima: " + max31 + "\nTemperatura minima: " + min31);

        System.out.println("\n--- MAXIMA TEMPERATURA AGRUPADA POR PROVINCIAS Y DIA");
        Map<String, Map<Integer, Optional<Double>>> temperaturaMaxporDiayProvincia = listaAemet.stream()
                .collect(groupingBy(
                        Aemet::getProvincia,
                        groupingBy(
                                Aemet::getDia,
                                mapping(
                                        Aemet::getTempMaxima,
                                        maxBy(Comparator.naturalOrder())))));

        temperaturaMaxporDiayProvincia.forEach((provincia, temperaturaPorDia) -> {
            System.out.println("Provincia: " + provincia);
            temperaturaPorDia.forEach((dia, temperaturaMaxima) -> {
                System.out.println("Dia: " + dia + ", Temperatura Maxima: " + temperaturaMaxima.orElse(0.0));
            });
            System.out.println("");
        });

        System.out.println("\n--- MINIMA TEMPERATURA AGRUPADA POR PROVINCIAS Y DIA");
        Map<String, Map<Integer, Optional<Double>>> temperaturaMinporDiayProvincia = listaAemet.stream()
                .collect(groupingBy(
                        Aemet::getProvincia,
                        groupingBy(
                                Aemet::getDia,
                                mapping(
                                        Aemet::getTempMaxima,
                                        minBy(Comparator.naturalOrder())))));

        temperaturaMinporDiayProvincia.forEach((provincia, temperaturaPorDia) -> {
            System.out.println("Provincia: " + provincia);
            temperaturaPorDia.forEach((fecha, temperaturaMinima) -> {
                System.out.println("Dia: " + fecha + ", Temperatura Maxima: " + temperaturaMinima.orElse(0.0));
            });
        });

        System.out.println("\n--- MEDIA DE TEMPERATURA AGRUPADA POR PROVINCIAS Y DIA");
        var tempMediaDiaProvincia = listaAemet.stream()
                .collect(groupingBy(x -> x.getProvincia() + "_" + x.getDia(),
                        averagingDouble(x -> (x.getTempMaxima() + x.getTempMinima()) / 2)));

        tempMediaDiaProvincia.forEach((clave, tempMedia) -> {
            String[] partes = clave.split("_");
            String provincia = partes[0];
            int dia = Integer.parseInt(partes[1]);
            System.out.println("Provincia: " + provincia + " Dia: " + dia + ", Temperatura media: " + tempMedia);
        });

        System.out.println("\n--- PRECIPITACION MAXIMA POR DIAS Y DONDE HA SIDO ----");
        Map<Integer, Map.Entry<String, Double>> precipitacionMaximaPorDiaYLocalidad = listaAemet.stream()
                .collect(groupingBy(
                        Aemet::getDia,
                        Collectors.collectingAndThen(
                                Collectors.maxBy(comparingDouble(Aemet::getPrecipitacion)),
                                maxPrecipitacion -> maxPrecipitacion.map(max -> new AbstractMap.SimpleEntry<>(max.getLocalidad(), max.getPrecipitacion())).orElse(null)
                        )
                ));

        precipitacionMaximaPorDiaYLocalidad.forEach((dia, localidadYMaxPrecipitacion) -> {
            System.out.println("Dia: " + dia);
            System.out.println("Localidad con mayor precipitacion: " + localidadYMaxPrecipitacion.getKey());
            System.out.println("Precipitacion maxima: " + localidadYMaxPrecipitacion.getValue());
            System.out.println("");
        });

        System.out.println("\n--- LUGARES DONDE HA LLOVIDO AGRUPADOS POR PROVINCIAS Y DIA ---");
        var lluviaProvinciaDia = listaAemet.stream()
                .filter(x -> x.getPrecipitacion() > 0)
                .collect(groupingBy(x -> x.getProvincia() + "_" + x.getDia(),
                        mapping(Aemet::getLocalidad, toList())));

        lluviaProvinciaDia.forEach((clave, lugar) -> {
            String[] partes = clave.split("_");
            String provincia = partes[0];
            int dia = Integer.parseInt(partes[1]);
            System.out.println("Provincia: " + provincia + " Dia: " + dia + ", Localidad: " + lugar);

        });

        System.out.println("\n--- LUGAR DONDE MAS HA LLOVIDO ---");
        var masLluvia = listaAemet.stream()
                .max(Comparator.comparingDouble(Aemet::getPrecipitacion))
                .map(Aemet::getLocalidad).orElse("");
        System.out.println(masLluvia);

        datosProvincia("Madrid");

    }

    public void datosProvincia(String provincia){

    }

    public void generateJSON(){}
}
