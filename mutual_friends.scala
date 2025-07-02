import org.apache.spark.{SparkConf, SparkContext}

object MutualFriends {

  def pairs(str: Array[String]) = {
    val users = str(1).split(",")
    val user = str(0)
    for (friend <- users) yield {
      val pair = if (user < friend) (user, friend) else (friend, user)
      (pair, users)
    }
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("MutualFriends").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val data = sc.textFile("soc-LiveJournal1Adj.txt")
    val data1 = data.map(_.split("\t")).filter(_.length == 2)
    val pairCounts = data1.flatMap(pairs).reduceByKey(_.intersect(_))
    val p1 = pairCounts.map { case ((a, b), friends) => s"$a\t$b\t${friends.mkString(",")}" }

    p1.saveAsTextFile("output")

    var ans = ""

    val p2 = p1.map(_.split("\t")).filter(_.length == 3).filter(x => x(0) == "0" && x(1) == "4").flatMap(x => x(2).split(",")).collect()
    ans += s"0\t4\t${p2.mkString(",")}\n"

    val p3 = p1.map(_.split("\t")).filter(_.length == 3).filter(x => x(0) == "1" && x(1) == "2").flatMap(x => x(2).split(",")).collect()
    ans += s"1\t2\t${p3.mkString(",")}\n"

    val answer = sc.parallelize(Seq(ans))
    answer.saveAsTextFile("output1")

    sc.stop()
  }
}
