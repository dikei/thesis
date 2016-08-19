package pt.tecnico.spark.util;

/**
 * Runtime statistic
 */
public class AppRuntimeStatistic {
    private String name;

    private Double barrier;
    private Double barrierLower;
    private Double barrierUpper;
    private Double barrierStdDev;
    private Long barrierSample;

    private Double noBarrier;
    private Double noBarrierLower;
    private Double noBarrierUpper;
    private Double noBarrierStdDev;
    private Long noBarrierSample;

    private Boolean significantDifferent;

    private Double speedUp;
    private Double speedUpLower;
    private Double speedUpUpper;

    public AppRuntimeStatistic(String name,
                               Double barrier, Double barrierLower, Double barrierUpper,
                               Double barrierStdDev, Long barrierSample,
                               Double noBarrier, Double noBarrierLower, Double noBarrierUpper,
                               Double noBarrierStdDev, Long noBarrierSample,
                               Boolean significantDifferent,
                               Double speedUp, Double speedUpLower, Double speedUpUpper) {
        this.name = name;
        this.barrier = barrier;
        this.barrierLower = barrierLower;
        this.barrierUpper = barrierUpper;
        this.barrierStdDev = barrierStdDev;
        this.noBarrier = noBarrier;
        this.noBarrierLower = noBarrierLower;
        this.noBarrierUpper = noBarrierUpper;
        this.noBarrierStdDev = noBarrierStdDev;
        this.significantDifferent = significantDifferent;
        this.speedUp = speedUp;
        this.speedUpLower = speedUpLower;
        this.speedUpUpper = speedUpUpper;
        this.barrierSample = barrierSample;
        this.noBarrierSample = noBarrierSample;
    }

    public String getName() {
        return name;
    }

    public Double getBarrier() {
        return barrier;
    }

    public Double getBarrierLower() {
        return barrierLower;
    }

    public Double getBarrierUpper() {
        return barrierUpper;
    }

    public Double getBarrierStdDev() {
        return barrierStdDev;
    }

    public Double getNoBarrier() {
        return noBarrier;
    }

    public Double getNoBarrierLower() {
        return noBarrierLower;
    }

    public Double getNoBarrierUpper() {
        return noBarrierUpper;
    }

    public Double getNoBarrierStdDev() {
        return noBarrierStdDev;
    }

    public Boolean getSignificantDifferent() {
        return significantDifferent;
    }

    public Double getSpeedUp() {
        return speedUp;
    }

    public Double getSpeedUpLower() {
        return speedUpLower;
    }

    public Double getSpeedUpUpper() {
        return speedUpUpper;
    }

    public Long getBarrierSample() {
        return barrierSample;
    }

    public Long getNoBarrierSample() {
        return noBarrierSample;
    }
}
