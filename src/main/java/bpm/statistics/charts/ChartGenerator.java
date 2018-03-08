package bpm.statistics.charts;

import javafx.geometry.VerticalDirection;
import org.knowm.xchart.*;
import org.knowm.xchart.internal.chartpart.Chart;
import org.knowm.xchart.style.Styler;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

public class ChartGenerator {

    public boolean barChart(String name) {

        double[] xData = new double[]{0.0, 1.0, 2.0};
        double[] yData = new double[]{2.0, 1.0, 0.0};

        // Create Chart
        Chart chart = QuickChart.getChart(name, "X", "Y", "y(x)", xData, yData);

        try {
            BitmapEncoder.saveJPGWithQuality(chart, "example.jpg", 1);
            return true;
        } catch (IOException e) {
            return false;
        }
        // Show it
        //new SwingWrapper(chart).displayChart();
    }

    public static CategoryChart BarChart(ArrayList<String> tags, ArrayList<Double> values) {
        // Create Chart
        CategoryChart chart = new CategoryChartBuilder().width(800).height(600).xAxisTitle("Activities").yAxisTitle("Executions").theme(Styler.ChartTheme.GGPlot2).build();

        //Customize
        chart.getStyler().setXAxisLabelAlignment(Styler.TextAlignment.Right);
        chart.getStyler().setXAxisLabelRotation(45);

        // Series
        chart.addSeries("Act", tags, values);

        return chart;
    }

}
