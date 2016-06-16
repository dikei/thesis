package pt.tecnico.postprocessing;

import org.jfree.chart.axis.DateAxis;
import org.jfree.chart.axis.DateTickUnit;

import java.util.Date;

/**
 * Custom date axis
 */
public class CustomDateAxis extends DateAxis {

    public CustomDateAxis(String label) {
        super(label);
    }

    @Override
    public Date calculateLowestVisibleTickValue(DateTickUnit unit) {
        return getMinimumDate();
    }
}
