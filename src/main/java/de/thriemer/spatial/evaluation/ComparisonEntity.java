package de.thriemer.spatial.evaluation;

import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Entity
@NoArgsConstructor
@AllArgsConstructor
@Setter
@Getter
public class ComparisonEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    Long id;

    private String baseDatabase;
    private String comparedDatabase;
    private String scenario;
    private String type;
    private String param;
    private double speedUp;
    private double speedUpStd;
    private double speedUpSE;

}
