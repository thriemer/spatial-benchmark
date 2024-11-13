package de.thriemer.spatial.framework;

import org.springframework.stereotype.Service;

import java.lang.ref.WeakReference;
import java.util.Random;

@Service
public class Blackhole {
    private volatile Object obj1;
    private int tlr;
    private volatile int tlrMask;

    public Blackhole() {
        Random r = new Random(System.nanoTime());
        tlr = r.nextInt();
        tlrMask = 1;
        obj1 = new Object();
    }

    public void consumeFull(Object obj) {
        int tlrMask = this.tlrMask; // volatile read
        int tlr = (this.tlr = (this.tlr * 1664525 + 1013904223));
        if ((tlr & tlrMask) == 0) {
            // SHOULD ALMOST NEVER HAPPEN IN MEASUREMENT
            this.obj1 = new WeakReference<>(obj);
            this.tlrMask = (tlrMask << 1) + 1;
        }
    }

}
