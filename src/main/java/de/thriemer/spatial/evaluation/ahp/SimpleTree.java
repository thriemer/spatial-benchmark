package de.thriemer.spatial.evaluation.ahp;

import org.apache.commons.math3.util.Pair;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public class SimpleTree {
    public String criteriaName;
    public float weight;

    public List<SimpleTree> children;

    public String printLatex() {
        if (children == null || children.isEmpty()) {
            return "[ " + this + "] ";
        }
        return "[ " + this + " " + children.stream().map(SimpleTree::printLatex).collect(Collectors.joining()) + " ] ";
    }

    public String toString() {
        String name = criteriaName;
        if (name.length() > 14) {
            name = name.replaceAll(" ", "\\\\\\\\");
        }
        return name + "\\\\(%.2f)".formatted(weight);
    }

    Map<String, Float> propagate(Function<String, Map<String, Float>> leafLookup) {

        Map<String, Float> raw = new HashMap<>();

        if (children == null || children.isEmpty()) {
            raw = leafLookup.apply(criteriaName);
        } else {

            Map<String, List<Map.Entry<String, Float>>> childResults = children.stream()
                    .flatMap(c -> c.propagate(leafLookup).entrySet().stream()).collect(Collectors.groupingBy(Map.Entry::getKey));
            for (var e : childResults.entrySet()) {
                float sum = e.getValue().stream().map(Map.Entry::getValue).reduce(0f, Float::sum);
                raw.put(e.getKey(), sum);
            }
        }

        return raw.entrySet().stream()
                .map(entry -> new Pair<>(entry.getKey(), entry.getValue() * weight))
                .collect(Collectors.toMap(Pair::getKey, Pair::getValue));
    }

}
