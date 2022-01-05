package sample


import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.apache.log4j.LogManager
import org.apache.spark.rdd.RDD
import scala.util.Random


object graphSampleRW {

  def main(args: Array[String]) {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 3) {
      logger.error("Usage:\nsample.graphSample <input dir> <output dir> <fraction int>")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("Spark Graph Sample")//.setMaster("local[4]")
    val sc = new SparkContext(conf)

    // Delete output directory, only to ease local development; will not work on AWS. ===========
    val hadoopConf = new org.apache.hadoop.conf.Configuration
    val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
    try {
      hdfs.delete(new org.apache.hadoop.fs.Path(args(1)), true)
    } catch {
      case _: Throwable => {}
    }
    // ================


    //    val seed = args(3).toInt
    val sample_frac = args(2).toFloat
    val root_frac = 0.1 //fraction of graph nodes to use as roots for random walks
    val numPartitions = 30
    val partitioner: HashPartitioner = new HashPartitioner(numPartitions)

    //initialize global accumulator
    val current_sample_size = sc.longAccumulator
    val edgeNo = sc.longAccumulator
    val vertexNo = sc.longAccumulator
    val iterNo = sc.longAccumulator

    val textFile = sc.textFile(args(0))

//    load graph as adjacency list and remove duplicate edges
    val graph: RDD[(Int, Array[Int])] = textFile.map { line =>
      val splitLine = line.split(" ")
      (splitLine(0).toInt, splitLine(1).toInt)
    }.distinct()
      .mapValues(x => Array(x))
      .reduceByKey((x, y) => x ++ y)
      .partitionBy(partitioner)
      .persist()

    //    get graph size
    graph.foreachPartition { iter =>
      iter.foreach { case (_, neighbours) =>
        vertexNo.add(1)
        edgeNo.add(neighbours.length)
      }
    }

    val sample_size = (edgeNo.value * sample_frac).toInt
    println("GRAPH SIZE: " + edgeNo.value + "    SAMPLE SIZE: " + sample_size)

    //initial sample consists of root_frac % of the graph nodes selected at random.
    // A random neighbor is selected from each adjacency list to complete the edge.
    var sample = graph.sample(false, root_frac).mapValues {
      neighbors => neighbors(Random.nextInt(neighbors.length))
    }.persist()

    sample.foreach(_ => current_sample_size.add(1))

    println("CURRENT SAMPLE SIZE: " + current_sample_size.value)

    var activeVertices = sample.map{case(x,y) => (y, 1)}

    var new_path: RDD[(Int, Int)] = sc.emptyRDD[(Int, Int)]

    do {
      iterNo.add(1)
//      join to graph to get adjacency lists of active edges and select one adjacency edge at random
      new_path = graph.join(activeVertices).mapValues {
        case (neighbors, _) => neighbors(Random.nextInt(neighbors.length))
      }.subtract(sample)

      new_path.foreach(_ => current_sample_size.add(1))
      println("CURRENT SAMPLE SIZE: " + current_sample_size.value)

      sample = sample.union(new_path)
        .partitionBy(partitioner)
        .persist()

      activeVertices = new_path ++
        sample.sample(false,(1-current_sample_size.value/sample_size)*root_frac)
        .map { case (x, y) => (y, 1) }
          .distinct()
          .partitionBy(partitioner)

    } while (current_sample_size.value < sample_size)

    println("ITERATIONS : " + iterNo.value)
    sample.saveAsTextFile(args(1))

  }
}