package de.thriemer.spatial.framework;

import lombok.RequiredArgsConstructor;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

@RequiredArgsConstructor
@Component
public class Settings {

    private final Environment environment;

    public boolean runBenchmark(){
        return !environment.containsProperty("skip-benchmark");
    }

    public boolean cleanFaultyRuns(){
        return environment.containsProperty("clean-faulty-runs");
    }

}
