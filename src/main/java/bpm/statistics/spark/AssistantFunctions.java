package bpm.statistics.spark;

import com.mongodb.spark.MongoSpark;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.*;
import org.apache.spark.storage.StorageLevel;
import org.bson.Document;
import scala.Serializable;
import scala.Tuple2;
import bpm.statistics.Interval;

import java.text.SimpleDateFormat;
import java.util.*;

import static bpm.log_editor.parser.Constants.*;
import static bpm.log_editor.parser.ConstantsImpl.D_TASKS;

public class AssistantFunctions {
    private static Interval bdInterval = null;
    public static JavaRDD<Document> initialRDD;
    public static JavaRDD<Document> tracesRDD;
    private static JavaPairRDD<Object, List<Tuple2<Object, List<Long>>>> orderedTraces;

    /*--------------------------------------------------------------------
                              FUNCTIONS
    --------------------------------------------------------------------*/

    public static JavaRDD<Document> getInitialRDD() {
        return initialRDD;
    }

    public static void setInitialRDD(JavaRDD<Document> rdd) {
        //Only use 1 time so we don't cache it
        initialRDD = rdd.persist(StorageLevel.MEMORY_ONLY());
    }

    public static void setTracesRDD(JavaRDD<Document> tracesRDD) {
        AssistantFunctions.tracesRDD = tracesRDD.persist(StorageLevel.MEMORY_ONLY());
    }

    public static JavaRDD<Document> getTracesRDD() {
        return tracesRDD;
    }

    /*Order activities by trace and timestamp creating a new RDD for upload logs and standardize them*/
    public static JavaRDD<Document> orderRDD(String format, HashMap<String, String> pairing, JavaRDD<Document> rdd) {

        JavaPairRDD<Tuple2<String, Long>, Document> preOrder = rdd.mapToPair(new PairFunction<Document, Tuple2<String, Long>, Document>() {
            SimpleDateFormat sdf = new SimpleDateFormat(format);
            Calendar c = Calendar.getInstance();

            @Override
            public Tuple2<Tuple2<String, Long>, Document> call(Document document) throws Exception {
                if (document.get(COMPLETE_TIME) != null) {
                    Tuple2 t = new Tuple2(document.getString(TRACE), document.get(COMPLETE_TIME));
                    return new Tuple2<Tuple2<String, Long>, Document>(t, document);
                } else {
                    Date f = sdf.parse(document.getString(pairing.get(COMPLETE_TIME)));
                    Date f2 = sdf.parse(document.getString(pairing.get(INITIAL_TIME)));

                    //Change timestamp format
                    c.setTime(f);
                    long time = c.getTimeInMillis();
                    c.setTime(f2);
                    long time2 = c.getTimeInMillis();

                    //Filter values on the new document and save with the standard names
                    Document doc = new Document(document);
                    doc.remove("_id");
                    doc.remove(pairing.get(TRACE));
                    doc.remove(pairing.get(ACTIVITY));
                    doc.remove(pairing.get(INITIAL_TIME));
                    doc.remove(pairing.get(COMPLETE_TIME));
                    doc.put(TRACE, document.getString(pairing.get(TRACE)));
                    doc.put(ACTIVITY, document.getString(pairing.get(ACTIVITY)));
                    //Change format of the new time
                    doc.put(INITIAL_TIME, time2);
                    doc.put(COMPLETE_TIME, time);

                    //Save a tuple to order by COMPLETE_TIME
                    Tuple2 t = new Tuple2(document.getString(pairing.get(TRACE)), time);
                    return new Tuple2<Tuple2<String, Long>, Document>(t, doc);
                }
            }
        }).sortByKey(new TupleMapLongComparator(), true, NUM_PARTITIONS);

        JavaRDD<Document> orderRDD = preOrder.map(new Function<Tuple2<Tuple2<String, Long>, Document>, Document>() {
            @Override
            public Document call(Tuple2<Tuple2<String, Long>, Document> v1) throws Exception {
                return v1._2;
            }
        });

        return orderRDD;
    }

    /*Order activities by trace and timestamp creating a new RDD for upload logs and standardize them*/
    public static JavaRDD<Document> standarizeRDD(String format, HashMap<String, String> pairing, JavaRDD<Document> rdd) {

        JavaRDD<Document> orderRDD = rdd.map(new Function<Document, Document>() {
            SimpleDateFormat sdf = new SimpleDateFormat(format);
            Calendar c = Calendar.getInstance();

            @Override
            public Document call(Document document) throws Exception {

                Date f = sdf.parse(document.getString(pairing.get(COMPLETE_TIME)));
                Date f2 = sdf.parse(document.getString(pairing.get(INITIAL_TIME)));

                //Change timestamp format
                c.setTime(f);
                long time = c.getTimeInMillis();
                c.setTime(f2);
                long time2 = c.getTimeInMillis();

                //Filter values on the new document and save with the standard names
                Document doc = new Document(document);
                doc.remove("_id");
                doc.remove(pairing.get(TRACE));
                doc.remove(pairing.get(ACTIVITY));
                doc.remove(pairing.get(INITIAL_TIME));
                doc.remove(pairing.get(COMPLETE_TIME));
                doc.put(TRACE, document.getString(pairing.get(TRACE)));
                doc.put(ACTIVITY, document.getString(pairing.get(ACTIVITY)));
                //Change format of the new time
                doc.put(INITIAL_TIME, time2);
                doc.put(COMPLETE_TIME, time);

                return doc;
            }
        });

        return orderRDD;
    }

    static class TupleMapLongComparator implements Comparator<Tuple2<String, Long>>, Serializable {
        @Override
        public int compare(Tuple2<String, Long> tuple1, Tuple2<String, Long> tuple2) {
            //TODO compare with ints if with Strings don´t work
            /*int result = tuple1._1.compareTo(tuple2._1);
            if (result < 0) {
                return -1;
            } else if (result > 0) {
                return 1;
            } else {
                int r2 = tuple1._2.compareTo(tuple2._2);
                if (r2 < 0) {
                    return -1;
                } else if (r2 > 0) {
                    return 1;
                } else {
                    return 0;
                }
            }*/

            if (Float.valueOf(tuple1._1) < Float.valueOf(tuple2._1)) {
                return -1;
            } else if (Float.valueOf(tuple1._1) > Float.valueOf(tuple2._1)) {
                return 1;
            } else {
                if (tuple1._2 < tuple2._2) {
                    return -1;
                } else if (tuple1._2 > tuple2._2) {
                    return 1;
                } else {
                    return 0;
                }
            }
        }
    }

    public static JavaRDD<Document> setConfig(String coll) {
        initialRDD = MongoSpark.load(SparkConnection.getContext(coll));
        return initialRDD;
    }

    //Get the first and last timestamp of the BD
    public static Interval calculateBDInterval() {
        //we need the first/final timestamp of the BD
        //Get min timestamp
        Document minTimestamp = initialRDD.reduce(new Function2<Document, Document, Document>() {
            @Override
            public Document call(Document arg0, Document arg1) throws Exception {
                Long timestamp1;
                Long timestamp2;
                timestamp1 = (long) arg0.get(INITIAL_TIME);
                timestamp2 = (long) arg1.get(INITIAL_TIME);


                if (timestamp1 > timestamp2) {
                    return arg1;
                } else {
                    return arg0;
                }
            }
        });

        long first = (long) minTimestamp.get(INITIAL_TIME);

        //Get max timestampf
        Document maxTimestampf = initialRDD.reduce(new Function2<Document, Document, Document>() {
            @Override
            public Document call(Document arg0, Document arg1) throws Exception {
                Long timestamp1 = (long) arg0.get(COMPLETE_TIME);
                Long timestamp2 = (long) arg1.get(COMPLETE_TIME);

                if (timestamp1 > timestamp2) {
                    return arg0;
                } else {
                    return arg1;
                }
            }
        });

        long last = (long) maxTimestampf.get(COMPLETE_TIME);
        bdInterval = new Interval(first, last);

        return bdInterval;
    }

    public static JavaRDD<Document> filter(Interval i) {
        Long first = i.getFirst();
        Long last = i.getLast();

        //Filter activities by timestamp
        JavaRDD<Document> filterLocal = initialRDD.filter(new Function<Document, Boolean>() {
            @Override
            public Boolean call(Document v1) throws Exception {
                long timestamp = 0l;
                timestamp = (long) v1.get(INITIAL_TIME);

                if (timestamp >= first && timestamp <= last) {
                    return true;
                } else {
                    return false;
                }
            }
        });

        return filterLocal;
    }

    //Get all different traces with the activities (ordered by timestamp)
    public static JavaRDD<Document> getAllActivities(JavaRDD<Document> rdd, boolean sameTime, boolean group) {
        //Beware. The final time is that of the 0 position and the initial time of the 1.

        //Save the trace, activity and timestamps
        JavaPairRDD<Object, List<Tuple2<Object, List<Long>>>> traces = rdd.mapToPair(new PairFunction<Document, Object, List<Tuple2<Object, List<Long>>>>() {
            @Override
            public Tuple2<Object, List<Tuple2<Object, List<Long>>>> call(Document document) throws Exception {
                List<Tuple2<Object, List<Long>>> l = new ArrayList();
                List<Long> timestamps = new ArrayList<>();
                timestamps.add((long) document.get(COMPLETE_TIME));
                timestamps.add((long) document.get(INITIAL_TIME));
                l.add(new Tuple2<>(document.getString(ACTIVITY), timestamps));
                return new Tuple2<Object, List<Tuple2<Object, List<Long>>>>(document.getString(TRACE), l);
            }
        });

        //Union all activities of the same trace (and order them)
        orderedTraces = traces.reduceByKey(new Function2<List<Tuple2<Object, List<Long>>>, List<Tuple2<Object, List<Long>>>, List<Tuple2<Object, List<Long>>>>() {
            @Override
            public List<Tuple2<Object, List<Long>>> call(List<Tuple2<Object, List<Long>>> v1, List<Tuple2<Object, List<Long>>> v2) throws Exception {
                List<Tuple2<Object, List<Long>>> union = new ArrayList<>();

                union.addAll(v1);

                boolean inserted;
                for (Tuple2<Object, List<Long>> e: v2) {
                    Long b = e._2.get(1);
                    inserted = false;

                    for (int pos=0; pos<union.size(); pos++) {
                        List<Long> times = union.get(pos)._2;
                        Long a = times.get(1);
                        if (a > b) {
                            union.add(pos, e);
                            inserted = true;
                            break;
                        }
                    }
                    if (!inserted) {
                        union.add(union.size(), e);
                    }
                }
                return union;
            }
        });

        //Cache data
        orderedTraces.persist(StorageLevel.MEMORY_ONLY());

        //Group identical consecutive tasks
        if (group) {
            orderedTraces = orderedTraces.mapValues(new Function<List<Tuple2<Object, List<Long>>>, List<Tuple2<Object, List<Long>>>>() {
                @Override
                public List<Tuple2<Object, List<Long>>> call(List<Tuple2<Object, List<Long>>> v1) throws Exception {
                    List<Tuple2<Object, List<Long>>> returnList = new ArrayList<>();
                    Object previousActivity = "";
                    Long firstTime = 0l;
                    Long lastTime = 0l;
                    Boolean repetidas = false;
                    for (Tuple2<Object, List<Long>> t : v1) {
                        if (previousActivity.equals(t._1)) {
                            //Do nothing with the repeated activity
                            repetidas = true;
                            lastTime = t._2.get(0);
                        } else if (repetidas) {
                            //Remove the first insertion of the repeated activity
                            returnList.remove(returnList.size() - 1);

                            //Define the initial and final time of the repeated activity
                            List<Long> times = new ArrayList<>();
                            times.add(lastTime);
                            times.add(firstTime);
                            Tuple2<Object, List<Long>> tuple = new Tuple2<>(previousActivity, times);
                            returnList.add(tuple);

                            //Add also the new activity
                            returnList.add(t);
                            previousActivity = t._1;
                            firstTime = t._2.get(1);

                            //Reset variables
                            repetidas = false;
                        } else {
                            previousActivity = t._1;
                            firstTime = t._2.get(1);
                            returnList.add(t);
                        }
                    }

                    if (repetidas) {
                        //Remove the first insertion of the repeated activity
                        returnList.remove(returnList.size() - 1);

                        //Define the initial and final time of the repeated activity
                        List<Long> times = new ArrayList<>();
                        times.add(lastTime);
                        times.add(firstTime);
                        Tuple2<Object, List<Long>> tuple = new Tuple2<>(previousActivity, times);
                        returnList.add(tuple);
                    }

                    return returnList;
                }
            });
        }

        //System.out.println("Partitions after reduce map: " + orderedTraces.getNumPartitions());

        /************Trace Stats****************/
        //Delete timestamps and put the activities as key (to delete duplicated values)
        JavaPairRDD<List<Object>, List<Object>> reverseFinalMap = orderedTraces.mapToPair(new PairFunction<Tuple2<Object, List<Tuple2<Object, List<Long>>>>, List<Object>, List<Object>>() {
            @Override
            public Tuple2<List<Object>, List<Object>> call(Tuple2<Object, List<Tuple2<Object, List<Long>>>> v1) throws Exception {
                List<Object> activities = new ArrayList<>();
                for (Tuple2<Object, List<Long>> t:v1._2) {
                    activities.add(t._1);
                }
                List<Object> l = new ArrayList<>();
                l.add(v1._1);
                return new Tuple2<List<Object>, List<Object>>(activities, l);
            }
        });


        //Cache data
        reverseFinalMap.persist(StorageLevel.MEMORY_ONLY());

        TracesStatistics.tracesCount = reverseFinalMap.count();

        //Reduce duplicated traces (save activities and all ids)
        JavaPairRDD<List<Object>, List<Object>> reverserFinalMap2 = reverseFinalMap.reduceByKey(new Function2<List<Object>, List<Object>, List<Object>>() {
            @Override
            public List<Object> call(List<Object> v1, List<Object> v2) throws Exception {
                List<Object> l = new ArrayList<>();
                l.addAll(v1);
                l.addAll(v2);
                return l;
            }
        });

        reverseFinalMap.unpersist();
        reverserFinalMap2.persist(StorageLevel.MEMORY_ONLY());

        //We use this for trace bpm.statistics
        TracesStatistics.allTracesPair = reverserFinalMap2;

        /***************************************/

        JavaRDD<Document> activities = orderedTraces.flatMap(new FlatMapFunction<Tuple2<Object, List<Tuple2<Object, List<Long>>>>, Document>() {
            @Override
            public Iterator<Document> call(Tuple2<Object, List<Tuple2<Object, List<Long>>>> v1) throws Exception {
                Long previous = v1._2.get(0)._2.get(1);
                List<Document> docs = new ArrayList<>();

                Document d = new Document();
                if (D_TASKS) {
                    d.put(TRACE, v1._1);
                    d.put(ACTIVITY, S_DUMMY_TASK);
                    Long valueStart = previous - 1000;
                    d.put(INITIAL_TIME, valueStart);
                    d.put(COMPLETE_TIME, previous);

                    docs.add(d);
                }

                for(int i = 0; i < v1._2.size(); i++) {
                    Tuple2<Object, List<Long>> o = v1._2.get(i);
                    Long first = o._2.get(1);
                    Long last = o._2.get(0);

                    d = new Document();
                    d.put(TRACE, v1._1);
                    d.put(ACTIVITY, v1._2.get(i)._1);
                    if (sameTime) {
                        d.put(INITIAL_TIME, previous);
                        d.put(COMPLETE_TIME, last);
                    } else {
                        d.put(INITIAL_TIME, first);
                        d.put(COMPLETE_TIME, last);
                    }
                    previous = last;

                    docs.add(d);
                }

                if (D_TASKS) {
                    d = new Document();
                    d.put(TRACE, v1._1);
                    d.put(ACTIVITY, E_DUMMY_TASK);
                    d.put(INITIAL_TIME, previous);
                    d.put(COMPLETE_TIME, previous + 1000);

                    docs.add(d);
                }

                return docs.iterator();
            }
        });

        //System.out.println("Partitions activities " + activities.getNumPartitions());

        return activities;
    }

    public static JavaRDD<Document> getAllTraces(boolean sameTime) {
        JavaRDD<Document> tracesBD = orderedTraces.map(new Function<Tuple2<Object, List<Tuple2<Object, List<Long>>>>, Document>() {
            @Override
            public Document call(Tuple2<Object, List<Tuple2<Object, List<Long>>>> v1) throws Exception {
                Long firstActivity;
                Long previous;
                firstActivity = v1._2.get(0)._2.get(1);
                previous = firstActivity;

                if (sameTime) {
                    firstActivity = previous;
                }

                Document d = new Document();
                d.put(TRACE, v1._1);

                //Map<String, List<Long>> activities = new LinkedHashMap<>();
                List<String> activities = new ArrayList<>();
                List<List<Long>> activitiesTimes = new ArrayList<>();

                List<Long> t;
                if (DUMMY_TASKS) {
                    t = new ArrayList<>();
                    Long valueStart = previous - 1000;
                    t.add(valueStart);
                    t.add(previous);
                    activities.add(S_DUMMY_TASK);
                    activitiesTimes.add(t);
                }

                Long last = 0l;
                for(int i = 0; i < v1._2.size(); i++) {
                    Tuple2<Object, List<Long>> o = v1._2.get(i);
                    Long first = o._2.get(1);
                    last = o._2.get(0);

                    t = new ArrayList<>();
                    if (sameTime) {
                        t.add(previous);
                        t.add(last);
                    } else {
                        t.add(first);
                        t.add(last);
                    }

                    previous = last;
                    activities.add((String) v1._2.get(i)._1);
                    activitiesTimes.add(t);
                }


                if (DUMMY_TASKS) {
                    t = new ArrayList<>();
                    t.add(previous);
                    t.add(previous + 1000);
                    activities.add(E_DUMMY_TASK);
                    activitiesTimes.add(t);
                }

                d.put(ACTIVITIES, activities);
                d.put(ACTIVITIES_TIME, activitiesTimes);
                d.put(FIRST_ACTIVITY_TIME, firstActivity);
                d.put(LAST_ACTIVITY_TIME, last);
                return d;
            }
        });

        //System.out.println("Partitions traces " + tracesBD.getNumPartitions());

        //Unpersist data
        orderedTraces.unpersist();
        return tracesBD;
    }

    //Return the total time of the log
    public static Long getLogTime() {
        long total = bdInterval.getLast() - bdInterval.getFirst();
        return total;
    }
}