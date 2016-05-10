package pt.tecnico.postprocessing;

import org.jfree.chart.renderer.category.GanttRenderer;

import java.awt.*;

class StageGanttRenderer extends GanttRenderer {

    private static final int PASS = 1; // 1 for normal draw, 2 for shadow
    private int row;
    private int col;
    private int index;
    private Color[] pallete = new Color[] {
            new Color(228,26,28),
            new Color(55,126,184),
            new Color(77,175,74),
            new Color(152,78,163)
    };

    public StageGanttRenderer() {
    }

    @Override
    public Paint getItemPaint(int row, int col) {
        if (this.row != row || this.col != col) {
            this.row = row;
            this.col = col;
            index = 0;
        }
        int clutIndex = index++ / PASS;
        return pallete[clutIndex];
    }
}