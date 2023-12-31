package org.example.controllers;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.example.database.DatabaseManager;
import org.example.serializers.LocalTimeDeserializer;
import org.example.models.Aemet;
import org.example.repository.aemet.AemetRepositoryImpl;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.time.LocalTime;
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
        generateJSON("Madrid");
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
                .max(Comparator.comparingDouble(Aemet::getTempMaxima))
                .map(Aemet::getLocalidad).orElse("");
        var min29 = listaAemet.stream()
                .filter(x -> x.getDia() == 29)
                .min(Comparator.comparingDouble(Aemet::getTempMinima))
                .map(Aemet::getLocalidad).orElse("");
        System.out.println("Temperatura maxima: " + max29 + "\nTemperatura minima: " + min29);

        System.out.println("\n--- DIA 30 ---");
        var max30 = listaAemet.stream()
                .filter(x -> x.getDia() == 30)
                .max(Comparator.comparingDouble(Aemet::getTempMaxima))
                .map(Aemet::getLocalidad).orElse("");
        var min30 = listaAemet.stream()
                .filter(x -> x.getDia() == 30)
                .min(Comparator.comparingDouble(Aemet::getTempMinima))
                .map(Aemet::getLocalidad).orElse("");
        System.out.println("Temperatura maxima: " + max30 + "\nTemperatura minima: " + min30);

        System.out.println("\n--- DIA 31 ---");
        var max31 = listaAemet.stream()
                .filter(x -> x.getDia() == 30)
                .max(Comparator.comparingDouble(Aemet::getTempMaxima))
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

        //cambiar provincia para mostrar otros datos
        datosProvincia("Madrid");

    }

    public void datosProvincia(String provincia){
        System.out.println("\n--- DATOS DE LA PROVINCIA DE " + provincia.toUpperCase() + " ---");
        System.out.println("\n--- Temperatura maxima, minima y donde ha sido por dia ---");
        Map<Integer, Map.Entry<String, Double>> temperaturaMaximaPorDiaYLocalidad = listaAemet.stream()
                .filter(x -> x.getProvincia().equals(provincia))
                .collect(groupingBy(
                        Aemet::getDia,
                        Collectors.collectingAndThen(
                                Collectors.maxBy(comparingDouble(Aemet::getTempMaxima)),
                                maxTemperatura -> maxTemperatura.map(max -> new AbstractMap.SimpleEntry<>(max.getLocalidad(), max.getTempMaxima())).orElse(null)
                        )
                ));
        temperaturaMaximaPorDiaYLocalidad.forEach((dia, localidadYMaxTemperatura) -> {
            System.out.println("Dia: " + dia);
            System.out.println("Localidad con mayor temperatura: " + localidadYMaxTemperatura.getKey());
            System.out.println("Temperatura maxima: " + localidadYMaxTemperatura.getValue());
            System.out.println("");
        });

        Map<Integer, Map.Entry<String, Double>> temperaturaMinimaPorDiaYLocalidad = listaAemet.stream()
                .filter(x -> x.getProvincia().equals(provincia))
                .collect(groupingBy(
                        Aemet::getDia,
                        Collectors.collectingAndThen(
                                Collectors.minBy(comparingDouble(Aemet::getTempMinima)),
                                minTemperatura -> minTemperatura.map(min -> new AbstractMap.SimpleEntry<>(min.getLocalidad(), min.getTempMinima())).orElse(null)
                        )
                ));
        temperaturaMinimaPorDiaYLocalidad.forEach((dia, localidadYMinTemperatura) -> {
            System.out.println("Dia: " + dia);
            System.out.println("Localidad con menor temperatura: " + localidadYMinTemperatura.getKey());
            System.out.println("Temperatura minima: " + localidadYMinTemperatura.getValue());
            System.out.println("");
        });

        System.out.println("\n--- Temperatura media maxima ---");
        Map<Integer, Double> temperaturaMediaMaximaPorDia = listaAemet.stream()
                .filter(x -> x.getProvincia().equals(provincia))
                .collect(groupingBy(
                        Aemet::getDia, averagingDouble(Aemet::getTempMaxima))
                        );
        temperaturaMediaMaximaPorDia.forEach((dia, tempMediaMax) -> {
            System.out.println("Dia: " + dia);
            System.out.println("Temperatura media maxima: " + tempMediaMax);
            System.out.println("");
        });

        System.out.println("\n--- Temperatura media minima ---");
        Map<Integer, Double> temperaturaMediaMinimaPorDia = listaAemet.stream()
                .filter(x -> x.getProvincia().equals(provincia))
                .collect(groupingBy(
                        Aemet::getDia, averagingDouble(Aemet::getTempMinima))
                );
        temperaturaMediaMinimaPorDia.forEach((dia, tempMediaMin) -> {
            System.out.println("Dia: " + dia);
            System.out.println("Temperatura media minima: " + tempMediaMin);
            System.out.println("");
        });

        System.out.println("\n--- Precipitaci�n maxima y donde ha sido ---");
        Map<Integer, Map.Entry<String, Double>> precipitacionMaximaPorDiaYLocalidad = listaAemet.stream()
                .filter(x -> x.getProvincia().equals(provincia))
                .collect(groupingBy(
                        Aemet::getDia,
                        Collectors.collectingAndThen(
                                Collectors.maxBy(comparingDouble(Aemet::getPrecipitacion)),
                                maxPrecipitacion -> maxPrecipitacion.map(max -> new AbstractMap.SimpleEntry<>(max.getLocalidad(), max.getPrecipitacion())).orElse(null)
                        )
                ));
        precipitacionMaximaPorDiaYLocalidad.forEach((dia, localidadYMaxPrecip) -> {
            System.out.println("Dia: " + dia);
            System.out.println("Localidad con mayor temperatura: " + localidadYMaxPrecip.getKey());
            System.out.println("Temperatura maxima: " + localidadYMaxPrecip.getValue());
            System.out.println("");
        });

        System.out.println("\n--- Precipitacion media ---");
        Map<Integer, Double> precipitacionMediaPorDia = listaAemet.stream()
                .filter(x -> x.getProvincia().equals(provincia))
                .collect(groupingBy(
                        Aemet::getDia, averagingDouble(Aemet::getPrecipitacion))
                );
        precipitacionMediaPorDia.forEach((dia, precipMedia) -> {
            System.out.println("Dia: " + dia);
            System.out.println("Precipitacion media: " + precipMedia);
            System.out.println("");
        });

    }

    public void generateJSON(String provincia){

        var datosJSON = listaAemet.stream().filter(x -> x.getProvincia().equals(provincia)).toList();
        String file = Paths.get("").toAbsolutePath() + File.separator + "data" + File.separator + provincia.toLowerCase()+".json";

        GsonBuilder gsonBuilder = new GsonBuilder();
        gsonBuilder.registerTypeAdapter(LocalTime.class, new LocalTimeDeserializer());
        Gson gson = gsonBuilder.setPrettyPrinting().create();
        try (FileWriter writer = new FileWriter(file)) {
            gson.toJson(datosJSON, writer);
            System.out.println("Datos exportados con �xito a " + file);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
