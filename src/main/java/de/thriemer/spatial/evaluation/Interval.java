package de.thriemer.spatial.evaluation;

import jakarta.persistence.Embeddable;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.text.DecimalFormat;

@NoArgsConstructor
@AllArgsConstructor
@Getter
@Setter
@Embeddable
public class Interval {

    double min;
    double max;

    public boolean containsZero() {
        return min < 0 && max > 0;
    }

    public String toString(DecimalFormat df) {
        return "[" + df.format(min) + " ; " + df.format(max) + "]";
    }

}
