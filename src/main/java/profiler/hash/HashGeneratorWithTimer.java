package profiler.hash;

public abstract class HashGeneratorWithTimer implements HashGenerator {
    private long time;

    public HashGeneratorWithTimer() {
        this.time = 0;
    }

    public void addTime(long time) {
        this.time += time;
    }

    public long getTime() {
        return time;
    }
}
