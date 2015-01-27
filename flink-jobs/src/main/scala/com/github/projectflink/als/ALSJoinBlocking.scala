package com.github.projectflink.als

import java.lang

import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.io.{TypeSerializerInputFormat, TypeSerializerOutputFormat}
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.core.fs.Path
import org.apache.flink.core.memory.{DataOutputView, DataInputView}
import org.apache.flink.types.Value
import org.apache.flink.util.Collector
import org.apache.flink.api.common.functions.{Partitioner => FlinkPartitioner, GroupReduceFunction, CoGroupFunction}
import org.jblas.{Solve, SimpleBlas, FloatMatrix}

import scopt.OptionParser

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag
import scala.util.Random

// ======================================= ALS algorithm ===========================================

class ALSJoinBlocking(dop: Int, factors: Int, lambda: Double, iterations: Int, userBlocks: Int,
                      itemBlocks: Int, seed: Long, persistencePath: Option[String])
  extends Serializable {

  import ALSJoinBlocking._

  /**
   * Calculates the matrix factorization for the given sparse matrix ratings.
   *
   * @param ratings Sparse matrix to calculate the factorization for
   * @return Factorization containing the user and item matrix
   */
  def factorize(ratings: DataSet[Rating]): Factorization = {
    val blockIDPartitioner = new BlockIDPartitioner()

    val ratingsByUserBlock = ratings.map{
      rating =>
        val blockID = rating.user % userBlocks
        (blockID, rating)
    } partitionCustom(blockIDPartitioner, 0)

    val ratingsByItemBlock = ratings map {
      rating =>
        val blockID = rating.item % itemBlocks
        (blockID, new Rating(rating.item, rating.user, rating.rating))
    } partitionCustom(blockIDPartitioner, 0)

    val (uIn, uOut) = createBlockInformation(userBlocks, itemBlocks, ratingsByUserBlock,
      blockIDPartitioner)
    val (iIn, iOut) = createBlockInformation(itemBlocks, userBlocks, ratingsByItemBlock,
      blockIDPartitioner)

    val (userIn, userOut) = persistencePath match {
      case Some(path) => persist(uIn, uOut, path + "userIn", path + "userOut")
      case None => (uIn, uOut)
    }

    val (itemIn, itemOut) = persistencePath match {
      case Some(path) => persist(iIn, iOut, path + "itemIn", path + "itemOut")
      case None => (iIn, iOut)
    }

    val initialItems = itemOut.partitionCustom(blockIDPartitioner, 0).map{
      outInfos =>
        val blockID = outInfos._1
        val infos = outInfos._2

        (blockID, infos.elementIDs.map{
          id =>
            val random = new Random(id ^ seed)
            randomFactors(factors, random)
        })
    }.withConstantSet("0")

    // iteration to calculate the item matrix
    val items = initialItems.iterate(iterations) {
      items => {
        val users = updateFactors(userBlocks, items, itemOut, userIn, factors, lambda,
          blockIDPartitioner)
        updateFactors(itemBlocks, users, userOut, itemIn, factors, lambda, blockIDPartitioner)
      }
    }

    val pItems = persistencePath match {
      case Some(path) => persist(items, path + "items")
      case None => items
    }

    // perform last half-step to calculate the user matrix
    val users = updateFactors(userBlocks, pItems, itemOut, userIn, factors, lambda,
      blockIDPartitioner)

    new Factorization(unblock(users, userOut, blockIDPartitioner), unblock(pItems, itemOut,
      blockIDPartitioner))
  }

  /**
   * Calculates a single half step of the ALS optimization. The result is the new value for
   * either the user or item matrix, depending with which matrix the method was called.
   *
   * @param numUserBlocks Number of blocks in the respective dimension
   * @param items Fixed matrix value for the half step
   * @param itemOut Out information to know where to send the vectors
   * @param userIn In information for the cogroup step
   * @param factors Number of latent factors
   * @param lambda Regularization constant
   * @param blockIDPartitioner Custom Flink partitioner
   * @return New value for the optimized matrix (either user or item)
   */
  def updateFactors(numUserBlocks: Int,
                    items: DataSet[(Int, Array[Array[Float]])],
                    itemOut: DataSet[(Int, OutBlockInformation)],
                    userIn: DataSet[(Int, InBlockInformation)],
                    factors: Int,
                    lambda: Double, blockIDPartitioner: FlinkPartitioner[Int]):
  DataSet[(Int, Array[Array[Float]])] = {
    // send the item vectors to the blocks whose users have rated the items
    val partialBlockMsgs = itemOut.join(items).where(0).equalTo(0).
      withPartitioner(blockIDPartitioner).apply {
      (left, right, col: Collector[(Int, Int, Array[Array[Float]])]) => {
        val blockID = left._1
        val outInfo = left._2
        val factors = right._2
        var userBlock = 0
        var itemIdx = 0

        while(userBlock < numUserBlocks){
          itemIdx = 0
          val buffer = new ArrayBuffer[Array[Float]]
          while(itemIdx < outInfo.elementIDs.length){
            if(outInfo.outLinks(userBlock)(itemIdx)){
              buffer += factors(itemIdx)
            }
            itemIdx += 1
          }

          if(buffer.nonEmpty){
            // send update message to userBlock
            col.collect(userBlock, blockID, buffer.toArray)
          }

          userBlock += 1
        }
      }
    }

    // collect the partial update messages and calculate for each user block the new user vectors
    partialBlockMsgs.coGroup(userIn).where(0).equalTo(0).sortFirstGroup(1, Order.ASCENDING).
      withPartitioner(blockIDPartitioner).apply{
          new CoGroupFunction[(Int, Int, Array[Array[Float]]), (Int,
            InBlockInformation), (Int, Array[Array[Float]])](){

            // in order to save space, store only the upper triangle of the XtX matrix
            val triangleSize = (factors*factors - factors)/2 + factors
            val matrix = FloatMatrix.zeros(triangleSize)
            val fullMatrix = FloatMatrix.zeros(factors, factors)
            val userXtX = new ArrayBuffer[FloatMatrix]()
            val userXy = new ArrayBuffer[FloatMatrix]()
            val numRatings = new ArrayBuffer[Int]()

            override def coGroup(left: lang.Iterable[(Int, Int, Array[Array[Float]])],
                                 right: lang.Iterable[(Int, InBlockInformation)],
                                 collector: Collector[(Int, Array[Array[Float]])]): Unit = {
              // there is only one InBlockInformation per user block
              val inInfo = right.iterator().next()._2
              val updates = left.iterator()

              val numUsers = inInfo.elementIDs.length
              var blockID = -1

              var i = 0

              // clear old matrices and allocate new ones
              val matricesToClear = if (numUsers > userXtX.length) {
                val oldLength = userXtX.length

                while(i < (numUsers - oldLength)) {
                  userXtX += FloatMatrix.zeros(triangleSize)
                  userXy += FloatMatrix.zeros(factors)
                  numRatings.+=(0)

                  i += 1
                }

                oldLength
              } else {
                numUsers
              }

              i = 0
              while(i  < matricesToClear){
                numRatings(i) = 0
                userXtX(i).fill(0.0f)
                userXy(i).fill(0.0f)

                i += 1
              }

              var itemBlock = 0

              // build XtX matrices and Xy vector
              while(updates.hasNext){
                val update = updates.next()
                val blockFactors = update._3
                blockID = update._1

                var p = 0
                while(p < blockFactors.length){
                  val vector = new FloatMatrix(blockFactors(p))
                  outerProduct(vector, matrix, factors)

                  val (users, ratings) = inInfo.ratingsForBlock(itemBlock)(p)

                  var i = 0
                  while (i < users.length) {
                    numRatings(users(i)) += 1
                    userXtX(users(i)).addi(matrix)
                    SimpleBlas.axpy(ratings(i), vector, userXy(users(i)))

                    i += 1
                  }
                  p += 1
                }

                itemBlock += 1
              }

              val array = new Array[Array[Float]](numUsers)

              i = 0
              while(i < numUsers){
                generateFullMatrix(userXtX(i), fullMatrix, factors)

                var f = 0

                // add regularization constant
                while(f < factors){
                  fullMatrix.data(f*factors + f) += lambda.asInstanceOf[Float] * numRatings(i)
                  f += 1
                }

                // calculate new user vector
                array(i) = Solve.solvePositive(fullMatrix, userXy(i)).data

                i += 1
              }

              collector.collect((blockID, array))
            }
          }
    }.withConstantSetFirst("0").withConstantSetSecond("0")
  }

  /**
   * Creates the meta information needed to route the item and user vectors to the respective user
   * and item blocks.
   *
   * @param userBlocks
   * @param itemBlocks
   * @param ratings
   * @param blockIDPartitioner
   * @return
   */
  def createBlockInformation(userBlocks: Int, itemBlocks: Int, ratings: DataSet[(Int, Rating)],
                             blockIDPartitioner: BlockIDPartitioner):
  (DataSet[(Int, InBlockInformation)], DataSet[(Int, OutBlockInformation)]) = {
    val blockIDGenerator = new BlockIDGenerator(itemBlocks)

    val usersPerBlock = createUsersPerBlock(ratings)

    val outBlockInfos = createOutBlockInformation(ratings, usersPerBlock, itemBlocks,
      blockIDGenerator)

    val inBlockInfos = createInBlockInformation(ratings, usersPerBlock, blockIDGenerator)

    (inBlockInfos, outBlockInfos)
  }

  /**
   * Calculates the userIDs in ascending order of each user block
   *
   * @param ratings
   * @return
   */
  def createUsersPerBlock(ratings: DataSet[(Int, Rating)]): DataSet[(Int, Array[Int])] = {
    ratings.map{ x => (x._1, x._2.user)}.withConstantSet("0").groupBy(0).
      sortGroup(1, Order.ASCENDING).reduceGroup {
      users => {
        val result = ArrayBuffer[Int]()
        var id = -1
        var oldUser = -1

        while(users.hasNext) {
          val user = users.next()

          id = user._1

          if (user._2 != oldUser) {
            result.+=(user._2)
            oldUser = user._2
          }
        }

        val userIDs = result.toArray
        (id, userIDs)
      }
    }.withConstantSet("0")
  }

  /**
   * Creates for every user block the out-going block information. The out block information
   * contains for every item block a bitset which indicates which user vector has to be sent to
   * this block. If a vector v has to be sent to a block b, then bitsets(b)'s bit v is
   * set to 1, otherwise 0. Additionally the user IDataSet are replaced by the user vector's index value.
   *
   * @param ratings
   * @param usersPerBlock
   * @param itemBlocks
   * @param blockIDGenerator
   * @return
   */
  def createOutBlockInformation(ratings: DataSet[(Int, Rating)],
                                usersPerBlock: DataSet[(Int, Array[Int])],
                                itemBlocks: Int, blockIDGenerator: BlockIDGenerator):
  DataSet[(Int, OutBlockInformation)] = {
    ratings.coGroup(usersPerBlock).where(0).equalTo(0).apply {
      (ratings, users) =>
        val userIDs = users.next()._2
        val numUsers = userIDs.length

        val userIDToPos = userIDs.zipWithIndex.toMap

        val shouldDataSend = Array.fill(itemBlocks)(new scala.collection.mutable.BitSet(numUsers))
        var blockID = -1
        while (ratings.hasNext) {
          val r = ratings.next()

          val pos =
            try {
              userIDToPos(r._2.user)
            }catch{
              case e: NoSuchElementException =>
                throw new RuntimeException(s"Key ${r._2.user} not  found. BlockID $blockID. " +
                  s"Elements in block ${userIDs.take(5).mkString(", ")}. " +
                  s"UserIDList contains ${userIDs.contains(r._2.user)}.", e)
            }

          blockID = r._1
          shouldDataSend(blockIDGenerator(r._2.item))(pos) = true
        }

        (blockID, OutBlockInformation(userIDs, new OutLinks(shouldDataSend)))
    }.withConstantSetFirst("0").withConstantSetSecond("0")
  }

  /**
   * Creates for every user block the incoming block information. The incoming block information
   * contains the userIDs of the users in the respective block and for every item block a
   * BlockRating instance. The BlockRating instance describes for every incoming set of item
   * vectors of an item block, which user rated these items and what the rating was. For that
   * purpose it contains for every incoming item vector a tuple of an id array us and a rating
   * array rs. The array us contains the indices of the users having rated the respective
   * item vector with the ratings in rs.
   *
   * @param ratings
   * @param usersPerBlock
   * @param blockIDGenerator
   * @return
   */
  def createInBlockInformation(ratings: DataSet[(Int, Rating)],
                               usersPerBlock: DataSet[(Int, Array[Int])],
                               blockIDGenerator: BlockIDGenerator):
  DataSet[(Int, InBlockInformation)] = {
    // Group for every user block the users which have rated the same item and collect their ratings
    val partialInInfos = ratings.map { x => (x._1, x._2.item, x._2.user, x._2.rating)}
      .withConstantSet("0").groupBy(0, 1).reduceGroup {
      x =>
        var userBlockID = -1
        var itemID = -1
        val userIDs = ArrayBuffer[Int]()
        val ratings = ArrayBuffer[Float]()

        while (x.hasNext) {
          val (uBlockID, item, user, rating) = x.next
          userBlockID = uBlockID
          itemID = item

          userIDs += user
          ratings += rating
        }

        (userBlockID, blockIDGenerator(itemID), itemID, (userIDs.toArray, ratings.toArray))
    }.withConstantSet("0")

    // Aggregate all ratings for items belonging to the same item block. Sort ascending with
    // respect to the itemID, because later the item vectors of the update message are sorted
    // accordingly.
    val collectedPartialInfos = partialInInfos.groupBy(0, 1).sortGroup(2, Order.ASCENDING).
      reduceGroup {
      new GroupReduceFunction[(Int, Int, Int, (Array[Int], Array[Float])), (Int,
        Int, Array[(Array[Int], Array[Float])])](){
        val buffer = new ArrayBuffer[(Array[Int], Array[Float])]

        override def reduce(iterable: lang.Iterable[(Int, Int, Int, (Array[Int],
          Array[Float]))], collector: Collector[(Int, Int, Array[(Array[Int],
          Array[Float])])]): Unit = {

          val infos = iterable.iterator()
          var counter = 0

          var blockID = -1
          var itemBlockID = -1

          while (infos.hasNext && counter < buffer.length) {
            val info = infos.next()
            blockID = info._1
            itemBlockID = info._2

            buffer(counter) = info._4

            counter += 1
          }

          while (infos.hasNext) {
            val info = infos.next()
            blockID = info._1
            itemBlockID = info._2

            buffer += info._4

            counter += 1
          }

          val array = new Array[(Array[Int], Array[Float])](counter)

          buffer.copyToArray(array)

          collector.collect((blockID, itemBlockID, array))
        }
      }
    }.withConstantSet("0", "1")

    // Aggregate all item block ratings with respect to their user block ID. Sort the blocks with
    // respect to their itemBlockID, because the block update messages are sorted the same way
    collectedPartialInfos.coGroup(usersPerBlock).where(0).equalTo(0).
      sortFirstGroup(1, Order.ASCENDING).apply{
      new CoGroupFunction[(Int, Int, Array[(Array[Int], Array[Float])]),
        (Int, Array[Int]), (Int, InBlockInformation)] {
        val buffer = ArrayBuffer[BlockRating]()

        override def coGroup(partialInfosIterable: lang.Iterable[(Int, Int, Array[(Array[Int], Array[Float])])],
                             userIterable: lang.Iterable[(Int, Array[Int])],
                             collector: Collector[(Int, InBlockInformation)]): Unit = {

          val users = userIterable.iterator()
          val partialInfos = partialInfosIterable.iterator()

          val userWrapper = users.next()
          val id = userWrapper._1
          val userIDs = userWrapper._2
          val userIDToPos = userIDs.zipWithIndex.toMap

          var counter = 0

          while (partialInfos.hasNext && counter < buffer.length) {
            val partialInfo = partialInfos.next()
            // entry contains the ratings and userIDs of a complete item block
            val entry = partialInfo._3

            // transform userIDs to positional indices
            for (row <- 0 until entry.length; col <- 0 until entry(row)._1.length) {
              entry(row)._1(col) = userIDToPos(entry(row)._1(col))
            }

            buffer(counter).ratings = entry

            counter += 1
          }

          while (partialInfos.hasNext) {
            val partialInfo = partialInfos.next()
            // entry contains the ratings and userIDs of a complete item block
            val entry = partialInfo._3

            // transform userIDs to positional indices
            for (row <- 0 until entry.length; col <- 0 until entry(row)._1.length) {
              entry(row)._1(col) = userIDToPos(entry(row)._1(col))
            }

            buffer += new BlockRating(entry)

            counter += 1
          }

          val array = new Array[BlockRating](counter)

          buffer.copyToArray(array)

          collector.collect((id, InBlockInformation(userIDs, array)))
        }
      }
    }.withConstantSetFirst("0").withConstantSetSecond("0")
  }

  /**
   * Unblocks the blocked user and item matrix representation so that it is at DataSet of
   * column vectors.
   *
   * @param users
   * @param outInfo
   * @param blockIDPartitioner
   * @return
   */
  def unblock(users: DataSet[(Int, Array[Array[Float]])],
              outInfo: DataSet[(Int, OutBlockInformation)],
              blockIDPartitioner: BlockIDPartitioner): DataSet[Factors] = {
    users.join(outInfo).where(0).equalTo(0).withPartitioner(blockIDPartitioner).apply {
      (left, right, col: Collector[Factors]) => {
        val outInfo = right._2
        val factors = left._2

        for(i <- 0 until outInfo.elementIDs.length){
          val id = outInfo.elementIDs(i)
          val factorVector = factors(i)
          col.collect(Factors(id, factorVector))
        }
      }
    }
  }

  // ================================ Math helper functions ========================================

  def outerProduct(vector: FloatMatrix, matrix: FloatMatrix, factors: Int): Unit = {
    val vd =  vector.data
    val md = matrix.data

    var row = 0
    var pos = 0
    while(row < factors){
      var col = 0
      while(col <= row){
        md(pos) = vd(row) * vd(col)
        col += 1
        pos += 1
      }

      row += 1
    }
  }

  def generateFullMatrix(triangularMatrix: FloatMatrix,
                         fullMatrix: FloatMatrix, factors: Int): Unit = {
    var row = 0
    var pos = 0
    val fmd = fullMatrix.data
    val tmd = triangularMatrix.data

    while(row < factors){
      var col = 0
      while(col < row){
        fmd(row*factors + col) = tmd(pos)
        fmd(col*factors + row) = tmd(pos)

        pos += 1
        col += 1
      }

      fmd(row*factors + row) = tmd(pos)

      pos += 1
      row += 1
    }
  }

  def generateRandomMatrix(users: DataSet[Int], factors: Int, seed: Long): DataSet[Factors] = {
    users map {
      id =>{
        val random = new Random(id ^ seed)
        Factors(id, randomFactors(factors, random))
      }
    }
  }

  def randomFactors(factors: Int, random: Random): Array[Float] ={
    Array.fill(factors)(random.nextDouble().asInstanceOf[Float])
  }

  // ========================= Convenience functions to persist DataSets ===========================

  def persist[T: ClassTag: TypeInformation](dataset: DataSet[T], path: String): DataSet[T] = {
    val env = dataset.getExecutionEnvironment
    val outputFormat = new TypeSerializerOutputFormat[T]

    val filePath = new Path(path)

    outputFormat.setOutputFilePath(filePath)
    outputFormat.setWriteMode(WriteMode.OVERWRITE)

    dataset.output(outputFormat)
    env.execute("FlinkTools persist")

    val inputFormat = new TypeSerializerInputFormat[T](dataset.getType.createSerializer())
    inputFormat.setFilePath(filePath)

    env.createInput(inputFormat)
  }

  def persist[A: ClassTag: TypeInformation ,B: ClassTag: TypeInformation](ds1: DataSet[A],
                                                                          ds2: DataSet[B],
                                                                          path1: String,
                                                                          path2: String):
  (DataSet[A], DataSet[B])  = {
    val env = ds1.getExecutionEnvironment

    val f1 = new Path(path1)

    val of1 = new TypeSerializerOutputFormat[A]
    of1.setOutputFilePath(f1)
    of1.setWriteMode(WriteMode.OVERWRITE)

    ds1.output(of1)

    val f2 = new Path(path2)

    val of2 = new TypeSerializerOutputFormat[B]
    of2.setOutputFilePath(f2)
    of2.setWriteMode(WriteMode.OVERWRITE)

    ds2.output(of2)

    env.execute("FlinkTools persist")

    val if1 = new TypeSerializerInputFormat[A](ds1.getType.createSerializer())
    if1.setFilePath(f1)

    val if2 = new TypeSerializerInputFormat[B](ds2.getType.createSerializer())
    if2.setFilePath(f2)

    (env.createInput(if1), env.createInput(if2))
  }
}

object ALSJoinBlocking {
  val USER_FACTORS_FILE = "userFactorsFile"
  val ITEM_FACTORS_FILE = "itemFactorsFile"

  // ==================================== ALS type definitions =====================================

  /**
   * Representation of a user-item rating
   *
   * @param user User ID of the rating user
   * @param item Item iD of the rated item
   * @param rating Rating value
   */
  case class Rating(user: Int, item: Int, rating: Float)

  /**
   * Representation of a factors vector of latent factor model
   *
   * @param id
   * @param factors
   */
  case class Factors(id: Int, factors: Array[Float]) {
    override def toString = s"($id, ${factors.mkString(",")})"
  }

  case class Factorization(userFactors: DataSet[Factors], itemFactors: DataSet[Factors])

  case class OutBlockInformation(elementIDs: Array[Int], outLinks: OutLinks) {
    override def toString: String = {
      s"OutBlockInformation:((${elementIDs.mkString(",")}), ($outLinks))"
    }
  }

  class OutLinks(var links: Array[scala.collection.mutable.BitSet]) extends Value {
    def this() = this(null)

    override def toString: String = {
      s"${links.mkString("\n")}"
    }

    override def write(out: DataOutputView): Unit = {
      out.writeInt(links.length)
      links foreach {
        link => {
          val bitMask = link.toBitMask
          out.writeInt(bitMask.length)
          for (element <- bitMask) {
            out.writeLong(element)
          }
        }
      }
    }

    override def read(in: DataInputView): Unit = {
      val length = in.readInt()
      links = new Array[scala.collection.mutable.BitSet](length)

      for (i <- 0 until length) {
        val bitMaskLength = in.readInt()
        val bitMask = new Array[Long](bitMaskLength)
        for (j <- 0 until bitMaskLength) {
          bitMask(j) = in.readLong()
        }
        links(i) = mutable.BitSet.fromBitMask(bitMask)
      }
    }

    def apply(idx: Int) = links(idx)
  }

  case class InBlockInformation(elementIDs: Array[Int], ratingsForBlock: Array[BlockRating]) {

    override def toString: String = {
      s"InBlockInformation:((${elementIDs.mkString(",")}), (${ratingsForBlock.mkString("\n")}))"
    }
  }

  case class BlockRating(var ratings: Array[(Array[Int], Array[Float])]) {
    def apply(idx: Int) = ratings(idx)

    override def toString: String = {
      ratings.map {
        case (left, right) => s"((${left.mkString(",")}),(${right.mkString(",")}))"
      }.mkString(",")
    }
  }

  case class BlockedFactorization(userFactors: DataSet[(Int, Array[Array[Float]])],
                                  itemFactors: DataSet[(Int, Array[Array[Float]])])

  class BlockIDPartitioner extends FlinkPartitioner[Int] {
    override def partition(blockID: Int, numberOfPartitions: Int): Int = {
      blockID % numberOfPartitions
    }
  }

  class BlockIDGenerator(blocks: Int) extends Serializable {
    def apply(id: Int): Int = {
      id % blocks
    }
  }

  // ========================================= Main method =========================================

  def main(args: Array[String]): Unit = {
    parseCL(args) map {
      config => {
        import config._

        val env = ExecutionEnvironment.getExecutionEnvironment

        val ratings = readRatings(inputRatings, env)

        val numBlocks = if (blocks <= 0) {
          env.getDegreeOfParallelism
        } else {
          blocks
        }

        val als = new ALSJoinBlocking(env.getDegreeOfParallelism, factors, lambda, iterations,
          numBlocks, numBlocks, seed, persistencePath)

        val blockFactorization = als.factorize(ratings)

        outputFactorization(blockFactorization, outputPath)

        env.execute("ALSJoinBlocking")
      }
    } getOrElse {
      println("Could not parse command line arguments.")
    }
  }

  def readRatings(input: String, env: ExecutionEnvironment): DataSet[Rating] = {
    if(input != null && input.nonEmpty){
      env.readCsvFile[(Int, Int, Float)](
        input,
        "\n",
        ','
      ) map { x => Rating(x._1, x._2, x._3)}
    }else {
      env.fromCollection(toyRatings).rebalance()
    }
  }

  def outputFactorization(factorization: Factorization, outputPath: String): Unit = {
    if (outputPath == null || outputPath.isEmpty) {
      factorization.userFactors.map("User: " + _).print()
      factorization.itemFactors.map("Item: " + _).print()
    } else {
      val path = if (outputPath.endsWith("/")) outputPath else outputPath + "/"
      val userPath = path + USER_FACTORS_FILE
      val itemPath = path + ITEM_FACTORS_FILE

      factorization.userFactors.writeAsText(
        userPath,
        WriteMode.OVERWRITE
      )

      factorization.itemFactors.writeAsText(
        itemPath,
        WriteMode.OVERWRITE
      )
    }
  }

  // ===================================== CLI parsing method ======================================

  case class ALSConfig(master: String = "local[4]",
                       factors: Int = -1, lambda: Double = 0.0,
                       iterations: Int = 0, inputRatings: String = null, outputPath: String = null,
                       blocks: Int = -1, seed: Long = -1, persistencePath: Option[String] = None)

  def parseCL(args: Array[String]): Option[ALSConfig] = {
    val parser = new OptionParser[ALSConfig]("ALS") {
      head("ALS", "1.0")
      arg[String]("master") action {
        (v, c) => c.copy(master = v)
      } text {
        "Master URL"
      }
      arg[Int]("factors") action {
        (v, c) => c.copy(factors = v)
      } text {
        "Number of factors"
      }
      arg[Double]("regularization") action {
        (v, c) => c.copy(lambda = v)
      } text {
        "Regularization constant"
      }
      arg[Int]("iterations") action {
        (v, c) => c.copy(iterations = v)
      }
      arg[Int]("blocks") action {
        (v, c) => c.copy(blocks = v)
      } text {
        "Number of blocks"
      }
      arg[String]("seed") action {
        (v, c) => {
          if (v.startsWith("rand")) {
            c.copy(seed = System.currentTimeMillis())
          } else {
            c.copy(seed = v.toLong)
          }

        }
      } text {
        "Seed for random initialization"
      }
      arg[String]("persistencePath") optional() action {
        (v, c) => {
          if (!v.toLowerCase.equals("none")) {
            c.copy(persistencePath = Some(if (v.endsWith("/")) v else v + "/"))
          } else {
            c
          }
        }
      } text {
        "Persistence path for the preprocessing data"
      }
      arg[String]("input") optional() action {
        (v, c) => c.copy(inputRatings = v)
      } text {
        "Path to input ratings"
      }
      arg[String]("output") optional() action {
        (v, c) => c.copy(outputPath = v)
      } text {
        "Output path for the results"
      }
    }

    parser.parse(args, ALSConfig())
  }

  // ================================= Toy data for local execution ================================

  def toyRatings: List[Rating] = {
    List(
      Rating(2,13,534.3937734561154f),
      Rating(6,14,509.63176469621936f),
      Rating(4,14,515.8246770897443f),
      Rating(7,3,495.05234565105f),
      Rating(2,3,532.3281786219485f),
      Rating(5,3,497.1906356844367f),
      Rating(3,3,512.0640508585093f),
      Rating(10,3,500.2906742233019f),
      Rating(1,4,521.9189079662882f),
      Rating(2,4,515.0734651491396f),
      Rating(1,7,522.7532725967008f),
      Rating(8,4,492.65683825096403f),
      Rating(4,8,492.65683825096403f),
      Rating(10,8,507.03319667905413f),
      Rating(7,1,522.7532725967008f),
      Rating(1,1,572.2230209271174f),
      Rating(2,1,563.5849190220224f),
      Rating(6,1,518.4844061038742f),
      Rating(9,1,529.2443732217674f),
      Rating(8,1,543.3202505434103f),
      Rating(7,2,516.0188923307859f),
      Rating(1,2,563.5849190220224f),
      Rating(1,11,515.1023793011227f),
      Rating(8,2,536.8571133978352f),
      Rating(2,11,507.90776961762225f),
      Rating(3,2,532.3281786219485f),
      Rating(5,11,476.24185144363304f),
      Rating(4,2,515.0734651491396f),
      Rating(4,11,469.92049343738233f),
      Rating(3,12,509.4713776280098f),
      Rating(4,12,494.6533165132021f),
      Rating(7,5,482.2907867916308f),
      Rating(6,5,477.5940040923741f),
      Rating(4,5,480.9040684364228f),
      Rating(1,6,518.4844061038742f),
      Rating(6,6,470.6605085832807f),
      Rating(8,6,489.6360564705307f),
      Rating(4,6,472.74052954447046f),
      Rating(7,9,482.5837650471611f),
      Rating(5,9,487.00175463269863f),
      Rating(9,9,500.69514584780944f),
      Rating(4,9,477.71644808419325f),
      Rating(7,10,485.3852917539852f),
      Rating(8,10,507.03319667905413f),
      Rating(3,10,500.2906742233019f),
      Rating(5,15,488.08215944254437f),
      Rating(6,15,480.16929757607346f)
    )
  }
}
