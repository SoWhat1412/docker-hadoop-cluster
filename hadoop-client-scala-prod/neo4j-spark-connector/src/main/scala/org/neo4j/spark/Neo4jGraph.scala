package org.neo4j.spark

import java.util
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.neo4j.driver.v1._
import org.neo4j.spark.Executor.execute

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

/**
  * @author mh
  * @since 06.03.16
  */
object Neo4jGraph {

  // nodeStmt: MATCH (n:Label) RETURN id(n) as id, n.foo as value UNION MATCH (m:Label2) return id(m) as id, m.foo as value
  // relStmt: MATCH (n:Label1)-[r:REL]->(m:Label2) RETURN id(n), id(m), r.foo // or id(r) or type(r) or ...
  def loadGraph[VD:ClassTag,ED:ClassTag](sc: SparkContext, nodeStmt: (String,Seq[(String, AnyRef)]), relStmt: (String,Seq[(String, AnyRef)])) :Graph[VD,ED] = {
    val nodes: RDD[(VertexId, VD)] =
      sc.makeRDD(execute(sc,nodeStmt._1,nodeStmt._2.toMap).rows.toSeq)
      .map(row => (row(0).asInstanceOf[Long],row(1).asInstanceOf[VD]))
    val rels: RDD[Edge[ED]] =
      sc.makeRDD(execute(sc,relStmt._1,relStmt._2.toMap).rows.toSeq)
      .map(row => new Edge[ED](row(0).asInstanceOf[VertexId],row(1).asInstanceOf[VertexId],row(2).asInstanceOf[ED]))
    Graph[VD,ED](nodes, rels)
  }

  def loadGraph[VD:ClassTag,ED:ClassTag](sc: SparkContext, nodeStmt: String, relStmt: String) :Graph[VD,ED] = {
    loadGraph(sc, (nodeStmt,Seq.empty),(relStmt,Seq.empty))
  }

  // label1, label2, relTypes are optional
  // MATCH (n:${name(label1}})-[via:${rels(relTypes)}]->(m:${name(label2)}) RETURN id(n) as from, id(m) as to
  def loadGraph(sc: SparkContext, label1: String, relTypes: Seq[String], label2: String) : Graph[Any,Int] = {
    def label(l : String) = if (l == null) "" else ":`"+l+"`"
    def rels(relTypes : Seq[String]) = relTypes.map(":`"+_+"`").mkString("|")

    val relStmt = s"MATCH (n${label(label1)})-[via${rels(relTypes)}]->(m${label(label2)}) RETURN id(n) as from, id(m) as to"

    loadGraphFromNodePairs[Any](sc,relStmt)
  }

  // MATCH (..)-[r:....]->(..) RETURN id(startNode(r)), id(endNode(r)), r.foo
  def loadGraphFromRels[VD:ClassTag,ED:ClassTag](sc: SparkContext, statement: String, parameters: Seq[(String, AnyRef)], defaultValue : VD = Nil) :Graph[VD,ED] = {
    val rels =
      sc.makeRDD(execute(sc, statement, parameters.toMap).rows.toSeq)
        .map(row => new Edge[ED](row(0).asInstanceOf[VertexId], row(1).asInstanceOf[VertexId],row(2).asInstanceOf[ED]))
      Graph.fromEdges[VD,ED](rels, defaultValue)
  }

  // MATCH (..)-[r:....]->(..) RETURN id(startNode(r)), id(endNode(r))
  def loadGraphFromNodePairs[VD:ClassTag](sc: SparkContext, statement: String, parameters: Seq[(String, AnyRef)] = Seq.empty, defaultValue : VD = Nil) :Graph[VD, Int] = {
    val rels: RDD[(VertexId, VertexId)] =
      sc.makeRDD(execute(sc,statement,parameters.toMap).rows.toSeq)
        .map(row => (row(0).asInstanceOf[Long],row(1).asInstanceOf[Long]))
    Graph.fromEdgeTuples[VD](rels, defaultValue = defaultValue)
  }

  def saveGraph[VD:ClassTag,ED:ClassTag](sc: SparkContext, graph: Graph[VD,ED], nodeProp : String = null, relTypeProp: (String,String) = null, mainLabelIdProp : Option[(String,String)] = None, secondLabelIdProp : Option[(String,String)] = None, merge: Boolean = false) : (Long,Long) = {
    val config = Neo4jConfig(sc.getConf)
    val matchMerge = if (merge) "MERGE" else "MATCH"
    val nodesUpdated : Long = nodeProp match {
      case null => 0
      case _ =>
        val updateNodes =
        mainLabelIdProp match {
          case Some((label, prop)) => 
            if (prop == "id") {
              s"UNWIND {data} as row $matchMerge (n:`$label`) WHERE id(n) = row.id SET n.`$nodeProp` = row.value return count(*)"
            } else {
              s"UNWIND {data} as row $matchMerge (n:`$label`) WHERE n.`$prop` = row.id SET n.`$nodeProp` = row.value return count(*)"
            }
            // s"UNWIND {data} as row $matchMerge (n:`$label`) WHERE n.`$prop` = row.id SET n.`$nodeProp` = row.value return count(*)"
          case None => s"UNWIND {data} as row MATCH (n) WHERE id(n) = row.id SET n.`$nodeProp` = row.value return count(*)"
        }
        val batchSize = ((graph.vertices.count() / 100) + 1).toInt
        graph.vertices.repartition(batchSize).mapPartitions[Long](
          p => {
            // TODO was toIterable instead of toList but bug in java-driver
            val rows = p.map(v => Seq(("id", v._1), ("value", v._2)).toMap.asJava).toList.asJava
            val res1 = execute(config, updateNodes, Map("data" -> rows)).rows
            val sum: Long = res1.map( x => x(0).asInstanceOf[Long]).sum
            Iterator.apply[Long](sum)
          }
        ).sum().toLong
    }

    val relsUpdated : Long = relTypeProp match {
      case null => 0
      case (relType,relProp) =>
        val updateRels =
          (mainLabelIdProp match {
            case Some((label, prop)) =>
              val (label2,prop2) = secondLabelIdProp.getOrElse(mainLabelIdProp.get)
              
	      var str1 = ""
	      if (prop == "id") {
                  str1 = s"MATCH (n:`$label`) WHERE id(n) = row.from "
              } else {
                  str1 = s"MATCH (n:`$label`) WHERE n.`$prop` = row.from "
              }

              var str2 = ""
              if (prop2 == "id") {
                  str2 = s"MATCH (m:`$label2`) WHERE id(m) = row.to"
              } else {
                  str2 = s"MATCH (m:`$label2`) WHERE m.`$prop2` = row.to"
              }

              // s"""UNWIND {data} as row
              //    |MATCH (n:`$label`) WHERE n.`$prop` = row.from
              //    |MATCH (m:`$label2`) WHERE m.`$prop2` = row.to""".stripMargin
              s"""UNWIND {data} as row
                  |$str1
                  |$str2""".stripMargin

            case None =>
              s"UNWIND {data} as row MATCH (n), (m) WHERE id(n) = row.from AND id(m) = row.to "
          }) +
            s" $matchMerge (n)-[rel:`$relType`]->(m) SET rel.`$relProp` = row.value return count(*)"
        val batchSize = ((graph.edges.count() / 100) + 1).toInt

        graph.edges.repartition(batchSize).mapPartitions[Long](
          p => {
            val rows = p.map(e => Seq(("from", e.srcId), ("to", e.dstId), ("value", e.attr)).toMap.asJava).toList.asJava
            val res1 = execute(config, updateRels, Map(("data", rows))).rows
            val sum : Long = res1.map(x => x(0).asInstanceOf[Long]).sum
            Iterator.apply[Long](sum)
          }
        ).sum().toLong
    }
    (nodesUpdated,relsUpdated) // todo
  }
}
