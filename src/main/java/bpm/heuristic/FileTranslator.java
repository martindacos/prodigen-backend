package bpm.heuristic;

import bpm.log_editor.data_types.Config;
import bpm.log_editor.data_types.MinedLog;
import bpm.log_editor.data_types.Node;
import bpm.log_editor.storage.MongoDAO;
import bpm.statistics.spark.ActivityStatisticsVO;
import bpm.statistics.spark.ArcStatisticsVO;
import bpm.statistics.spark.TraceStatisticsVO;
import es.usc.citius.womine.model.Arc;
import es.usc.citius.womine.model.Graph;
import es.usc.citius.womine.model.INode;
import es.usc.citius.womine.model.Pattern;
import scala.Tuple2;

import java.io.File;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.*;

import static bpm.log_editor.parser.Constants.LOG_DIR;

public class FileTranslator {
    private final static GraphViz gv = new GraphViz();

    public static String resultsToJSON(Set<Pattern> results, Boolean removeComplete) {
        Integer i, weight, numNodes, numArcs;
        String jsonResults;
        DecimalFormat df;
        DecimalFormatSymbols otherSymbols;

        otherSymbols = new DecimalFormatSymbols();
        otherSymbols.setDecimalSeparator('.');
        df = new DecimalFormat("##0.00", otherSymbols);

        i = 0;
        jsonResults = "[";
        for (Pattern pattern : results) {
            numNodes = pattern.getNodes().size();
            numArcs = pattern.getArcs().size();

            jsonResults += "\n{\n";
            jsonResults += "\"name\": \"Pattern " + i + "\",\n";
            jsonResults += "\"numTasks\": " + numNodes + ",\n";
            jsonResults += "\"frequency\": " + df.format(pattern.getFrequency() * 100) + ",\n";

            weight = numNodes * 2 + numArcs;
            jsonResults += "\"weight\": " + weight + ",\n";
            jsonResults += "\"nodes\": \"";
            for (INode node : pattern.getNodes()) {
                jsonResults += simplifyName(node.getId(), removeComplete) + ",";
            }
            // Delete last comma.
            jsonResults = jsonResults.substring(0, jsonResults.length() - 1);
            jsonResults += "\",\n";
            jsonResults += "\"arcs\": \"";
            for (Arc arc : pattern.getArcs()) {
                jsonResults += simplifyName(arc.getSource().getId(), removeComplete) + "->" + simplifyName(arc.getDestination().getId(), removeComplete) + ",";
            }
            if (pattern.getArcs().size() > 0) {
                // Delete last comma.
                jsonResults = jsonResults.substring(0, jsonResults.length() - 1);
            }
            jsonResults += "\",\n";

            jsonResults += "\"dot\": \"";
            jsonResults += ParserHNGraph.translate(pattern, removeComplete)
                    .replace("\\", "\\\\")
                    .replace("\"", "\\\"")
                    .replace("\n", "\\n")
                    .replace("\t", "\\t");
            jsonResults += "\"\n" +
                    "},";
            i++;
        }
        if (results.size() > 0) {
            // Delete last comma.
            jsonResults = jsonResults.substring(0, jsonResults.length() - 1);
        }
        jsonResults += "\n]\n";

        return jsonResults;
    }

    public static void resultsPatternsToDot(Graph model, Set<Pattern> results, Boolean removeComplete, String hnName,
                                            boolean infrequent, Double threshold, MinedLog r) {
        Integer i;
        DecimalFormat df;
        DecimalFormatSymbols otherSymbols;

        otherSymbols = new DecimalFormatSymbols();
        otherSymbols.setDecimalSeparator('.');
        df = new DecimalFormat("##0.00", otherSymbols);

        i = 0;
        List<String> patterns = new ArrayList<>();
        for (Pattern pattern : results) {
            String frequency = df.format(pattern.getFrequency() * 100);
            List<INode> nodesPattern = pattern.getNodes();
            Set<Arc> arcsPattern = pattern.getArcs();
            String p = ParserHNGraph.translatePaint(model, removeComplete, nodesPattern, arcsPattern, i, frequency, hnName, infrequent, threshold);
            patterns.add(p);
            i++;
        }

        //Save pattern on DB
        int d = (int) (threshold * 100);
        if (infrequent) {
            r.getIP2().put(d, patterns);
        } else {
            r.getFP2().put(d, patterns);
        }
    }

    public static void ActivityStatsToDot(Graph model, ActivityStatisticsVO VO, String hnName, Config c, MinedLog r) {
        String fil = "";
        if (c != null) {
            Node node = r.getNode();
            LinkedHashMap<String, ArrayList<String>> filter = node.getFilter();
            for (Map.Entry<String, ArrayList<String>> entry : filter.entrySet()) {
                fil += entry.getKey() + " " + entry.getValue();
                fil += " > ";
            }
            fil = fil.replaceAll(" > $", "");
        }

        String s = ParserHNGraph.normalTranslate(model, true, hnName, hnName, fil);
        //Save graph on DB
        r.setModel(s);

        List<INode> nodes = model.getNodes();
        List<String> nodesS = new ArrayList<>();
        for (INode n : nodes) {
            nodesS.add(n.getId());
        }
        List<Tuple2<String, Double>> activityFrequency = VO.activityFrequency;
        List<Object> activityAverages = VO.activityAverages;

        heatMapFrequency(model, activityFrequency, hnName, r, c, nodesS);

        List<List<Object>> activityLongerShorterDuration = VO.activityLongerShorterDuration;
        heatMapDuration(model, activityAverages, hnName, activityLongerShorterDuration, r, c, nodesS);

    }

    public static void heatMapFrequency(Graph model, List<Tuple2<String, Double>> activityFrequency, String hnName, MinedLog r, Config c, List<String> nodesS) {
        Double maxFrequency = activityFrequency.get(0)._2;
        Double chunksize = maxFrequency / 4;
        ArrayList<String> _0 = new ArrayList<>();
        ArrayList<String> _1 = new ArrayList<>();
        ArrayList<String> _2 = new ArrayList<>();
        ArrayList<String> _3 = new ArrayList<>();

        for (int i = 0; i <activityFrequency.size(); i++) {
            if (nodesS.contains(activityFrequency.get(i)._1 + ":complete")) {
                Double f = activityFrequency.get(i)._2;
                if (f < chunksize) {
                    _0.add(activityFrequency.get(i)._1);
                } else if (f < chunksize * 2) {
                    _1.add(activityFrequency.get(i)._1);
                } else if (f < chunksize * 3) {
                    _2.add(activityFrequency.get(i)._1);
                } else {
                    _3.add(activityFrequency.get(i)._1);
                }
            }
        }

        HashMap<String, ArrayList<String>> nodesToPaint = new HashMap<>();
        nodesToPaint.put("#fff3e0", _0);
        nodesToPaint.put("#ffcc80", _1);
        nodesToPaint.put("#ef6c00", _2);
        nodesToPaint.put("#d50000", _3);

        List<String> orderColor = new ArrayList<>();
        orderColor.add("#fff3e0");
        orderColor.add("#ffcc80");
        orderColor.add("#ef6c00");
        orderColor.add("#d50000");

        String activity_frequency_heatMap = ParserHNGraph.translateStats(model, true,  nodesToPaint, chunksize, false, orderColor, null);

        if (c!= null && c.isActivity_frequency_heatmap()) {
            File out = new File(LOG_DIR + hnName + "/" + "Activity_Frequency_HeatMap" + ".png");
            if (gv.writeGraphToFile( gv.getGraph(activity_frequency_heatMap, "png", "dot"), out) == -1) {
                System.out.println("ERROR");
            }
        }

        //Save graph on DB
        r.getActStats().activityFrequencyHeatMap = activity_frequency_heatMap;
    }

    public static void heatMapDuration(Graph model, List<Object> activityAverages, String hnName, List<List<Object>> activityLongerShorterDuration,
                                       MinedLog r, Config c, List<String> nodesS) {
        List<Object> list = (List<Object>) activityAverages.get(0);
        Long maxFrequency = (Long) list.get(1);
        Long chunksize = maxFrequency / 4;
        ArrayList<String> _0 = new ArrayList<>();
        ArrayList<String> _1 = new ArrayList<>();
        ArrayList<String> _2 = new ArrayList<>();
        ArrayList<String> _3 = new ArrayList<>();

        for (int i = 0; i <activityAverages.size(); i++) {
            List<Object> lista = (List<Object>) activityAverages.get(i);
            Long f = (Long) lista.get(1);
            String name = (String) lista.get(0);
            if (nodesS.contains(name + ":complete")) {
                if (f < chunksize) {
                    _0.add(name);
                } else if (f < chunksize * 2) {
                    _1.add(name);
                } else if (f < chunksize * 3) {
                    _2.add(name);
                } else {
                    _3.add(name);
                }
            }
        }

        HashMap<String, ArrayList<String>> nodesToPaint = new HashMap<>();
        nodesToPaint.put("#fff3e0", _0);
        nodesToPaint.put("#ffcc80", _1);
        nodesToPaint.put("#ef6c00", _2);
        nodesToPaint.put("#d50000", _3);

        List<String> orderColor = new ArrayList<>();
        orderColor.add("#fff3e0");
        orderColor.add("#ffcc80");
        orderColor.add("#ef6c00");
        orderColor.add("#d50000");

        String activity_duration_heatMap = ParserHNGraph.translateStats(model, true, nodesToPaint, chunksize.doubleValue(),
                true, orderColor, activityLongerShorterDuration);

        if (c!= null && c.isActivity_duration_heatmap()) {
            File out = new File(LOG_DIR + hnName + "/" + "Activity_Duration_HeatMap" + ".png");
            if (gv.writeGraphToFile( gv.getGraph(activity_duration_heatMap, "png", "dot"), out) == -1) {
                System.out.println("ERROR");
            }
        }

        //Save graph on DB
        r.getActStats().activityDurationHeatMap = activity_duration_heatMap;
    }

    public static void ArcStatsToDot(Graph model, ArcStatisticsVO arcStatisticsVO, String hnName, Config c, MinedLog r) {
        List<bpm.statistics.Arc> arcs = arcStatisticsVO.arcsFrequencyOrdered;

        Double maxFrequency = arcs.get(0).getFrequency();

        String arcs_frequencies_number = ParserHNGraph.translateArcsFrequencies(model, true, arcs);

        //Save on BD
        r.getArcStats().arcsFrequencyNumber = arcs_frequencies_number;

        if (c != null && c.isArcs_frequency_number()) {
            File out = new File(LOG_DIR + hnName + "/" + "Arcs_Frequencies_Number" + ".png");
            gv.writeGraphToFile( gv.getGraph(arcs_frequencies_number, "png", "dot"), out);
        }

        //Arcs Frequencies (HeatMap)
        Double chunksize = maxFrequency / 4;
        ArrayList<bpm.statistics.Arc> _0 = new ArrayList<>();
        ArrayList<bpm.statistics.Arc> _1 = new ArrayList<>();
        ArrayList<bpm.statistics.Arc> _2 = new ArrayList<>();
        ArrayList<bpm.statistics.Arc> _3 = new ArrayList<>();

        for (int i = 0; i <arcs.size(); i++) {
            bpm.statistics.Arc name = arcs.get(i);
            Double f = name.getFrequency();
            if (f < chunksize) {
                _0.add(name);
            } else if (f < chunksize * 2) {
                _1.add(name);
            } else if (f < chunksize * 3) {
                _2.add(name);
            } else {
                _3.add(name);
            }
        }
        HashMap<String, ArrayList<bpm.statistics.Arc>> arcsToPaint = new HashMap<>();
        arcsToPaint.put("#c1b29b", _0);
        arcsToPaint.put("#ffcc80", _1);
        arcsToPaint.put("#ef6c00", _2);
        arcsToPaint.put("#d50000", _3);

        List<String> orderColor = new ArrayList<>();
        orderColor.add("#c1b29b");
        orderColor.add("#ffcc80");
        orderColor.add("#ef6c00");
        orderColor.add("#d50000");

        String arcs_frequencies = ParserHNGraph.translateArcsStats(model, true, arcsToPaint, chunksize, orderColor, false);
        String arcs_frequencies_color_number = ParserHNGraph.translateArcsStats(model, true, arcsToPaint, chunksize, orderColor, true);

        //Save on BD
        r.getArcStats().arcsFrequencies = arcs_frequencies;
        r.getArcStats().arcsFrequencyColorNumber = arcs_frequencies_color_number;

        if (c != null && c.isArcs_frequency()) {
            File out = new File(LOG_DIR + hnName + "/" + "Arcs_Frequencies" + ".png");
            gv.writeGraphToFile( gv.getGraph(arcs_frequencies, "png", "dot"), out);

            out = new File(LOG_DIR + hnName + "/" + "Arcs_Frequencies_Color_Number" + ".png");
            gv.writeGraphToFile( gv.getGraph(arcs_frequencies_color_number, "png", "dot"), out);
        }

        if (c != null) {
            doPrune(c, arcs, hnName);
        }

        List<List<Object>> arcsLoops = arcStatisticsVO.arcsLoops;
        List<Object> loops = arcsLoops.get(0);
        List<Object> end_loops = arcsLoops.get(1);
        List<Object> nodes = arcsLoops.get(2);

        String arcs_loops = ParserHNGraph.translateArcsLoops(model, true, loops, end_loops, nodes, arcs);

        //Save on BD
        r.getArcStats().arcsLoopsGraph = arcs_loops;

        if (c!= null && c.isArcs_loops()) {
            File out = new File(LOG_DIR + hnName + "/" + "Arcs_Loops" + ".png");
            gv.writeGraphToFile(gv.getGraph(arcs_loops, "png", "dot"), out);
        }
    }

    public static void doPrune(Config c, List<bpm.statistics.Arc> arcs, String hnName) {
        Double maxFrequency = arcs.get(0).getFrequency();
        List<String> list1 = Arrays.asList(c.getPrune_Arcs().split(","));
        List<Double> prunado = MongoDAO.listToDoubleList(list1);
        if (c != null && prunado.size() > 0) {
            List<List<bpm.statistics.Arc>> pruneArcs = new ArrayList<>();
            List<Set<String>> pruneNodes = new ArrayList<>();

            for (int j = 0; j < prunado.size(); j++) {
                Double arcFrequencyLimit = maxFrequency * prunado.get(j);
                List<bpm.statistics.Arc> pArcs = new ArrayList<>();
                Set<String> pNodes = new HashSet<>();
                for (int i = 0; i < arcs.size(); i++) {
                    bpm.statistics.Arc name = arcs.get(i);
                    Double f = name.getFrequency();
                    if (f > arcFrequencyLimit) {
                        pArcs.add(name);
                        pNodes.add(name.getActivityA());
                        pNodes.add(name.getActivityB());
                    } else {
                        //No tiene sentido seguir explorando porque estan ordenados
                        break;
                    }
                }
                pruneArcs.add(pArcs);
                pruneNodes.add(pNodes);
            }

            for (int i=0; i<pruneArcs.size(); i++) {
                ParserHNGraph.translateArcsPrune(true, hnName, "Arcs_Prune_" + prunado.get(i), pruneArcs.get(i), pruneNodes.get(i));
            }
        }
    }

    public static void TraceStatsToDot(Graph model, TraceStatisticsVO VO, String hnName, Config c, MinedLog r) {
        List<Tuple2<List<Object>, List<Object>>> traceArcsFrequency = VO.traceArcsFrequency;
        List<Tuple2<List<Object>, Object>> traceRelativeFrequency = VO.traceRelativeFrequency;
        List<Tuple2<List<Object>, Object>> traceFrequency = VO.traceFrequency;
        List<List<Object>> traceLongerShorterDuration = VO.traceLongerShorterDuration;

        int i = 1;
        List<String> mostF = new ArrayList<>();
        for (Tuple2<List<Object>, List<Object>> tuple : traceArcsFrequency) {
            Tuple2<List<Object>, Object> fre = traceRelativeFrequency.get(i - 1);
            Tuple2<List<Object>, Object> fre2 = traceFrequency.get(i - 1);
            String mostFrequentPath = ParserHNGraph.translateTraceFrequency(model, true, tuple, fre._2, fre2._2, VO.tracesCount);

            //Save on the List to save on BD
            mostF.add(mostFrequentPath);

            if (c!= null && c.isMost_frequent_path()) {
                File out = new File(LOG_DIR + hnName + "/" + "MostFrequentPath" + "_"+ i +".png");
                gv.writeGraphToFile( gv.getGraph(mostFrequentPath, "png", "dot"), out);
            }
            i++;
        }

        //Save on BD
        r.getTraceStats().mostFrequentPath = mostF;

        List<Object> objects = traceLongerShorterDuration.get(0);
        List<Object> listaNodes = (List<Object>) objects.get(0);
        List<Object> listaArcs = (List<Object>) objects.get(3);
        Long duration = (Long) traceLongerShorterDuration.get(0).get(1);

        String critical_path = ParserHNGraph.translateCriticalTrace(model, true, listaNodes, listaArcs, duration);

        //Save on BD
        r.getTraceStats().criticalPath = critical_path;

        if (c!= null && c.isCritical_path()) {
            File out = new File(LOG_DIR + hnName + "/" + "CriticalPath" + ".png");
            gv.writeGraphToFile( gv.getGraph(critical_path, "png", "dot"), out);
        }
    }

    static String simplifyName(String name, Boolean removeComplete) {
        String simplifiedName = removeComplete
                ? name.replace(":complete", "").replace(":COMPLETE", "")
                : name;

        return simplifiedName.replaceAll("\\s", "_").replaceAll("-", "_").replaceAll(":", "__");
    }
}
