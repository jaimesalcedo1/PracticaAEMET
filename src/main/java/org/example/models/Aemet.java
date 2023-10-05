package org.example.models;

import lombok.Builder;
import lombok.Data;

import java.sql.Time;
import java.time.LocalTime;

@Data
@Builder
public class Aemet {
    private int cod;
    private int dia;
    private String localidad;
    private String provincia;
    private Double tempMaxima;
    private LocalTime horaTempMax;
    private Double tempMinima;
    private LocalTime horaTempMin;
    private Double precipitacion;

    @Override
    public String toString(){
        return "Aemet{" +
                "dia=" + dia +
                ", localidad='" + localidad+ '\'' +
                ", provincia='" + provincia + '\'' +
                ", temperatura m�xima='" + tempMaxima + '\'' +
                ", hora temperatura m�xima=" + horaTempMax +
                ", temperatura m�nima='" + tempMinima + '\'' +
                ", hora temperatura m�nima='" + horaTempMin + '\'' +
                ", precipitaci�n='" + precipitacion + '\'' +
                '}';
    }
}
