import java.util.Random;

public class Range {
    public double left;
    public double right;
    Range(double l, double r) {
        left =l; right = r;
    }
    public void update(double input) {
        if(input < left) left = input;
        if(input > right) right = input;
    }

    public double len() {
        return this.right - this.left;
    }

    public static Range randomBySelectivity(Range r, double selectivity) {
        Random rd = new Random();
        double lx = rd.nextDouble()*(1.0 - selectivity);
        return new Range(r.left + lx*r.len(), r.left + (lx+selectivity)*r.len());
    }

    public static Range createRange(double l, double r) {
        return new Range(l, r);
    }

    public static Range intersect(Range a, Range b) {
        return new Range(Double.max(a.left, b.left), Double.min(a.right, b.right));
    }
}
