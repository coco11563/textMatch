import java.io.{File, FileWriter}

import org.apdplat.word.WordSegmenter
import org.apdplat.word.segmentation.SegmentationAlgorithm

import scala.collection.mutable

class orgClass(val code:String, val id:String,
               val name: String, val briefName:String,
               val city : String, val province:String) {
  var nameSet : mutable.HashSet[String] = new mutable.HashSet[String]()
  var briefSet : mutable.HashSet[String] = new mutable.HashSet[String]()
  WordSegmenter.seg(name, SegmentationAlgorithm.MinimumMatching).forEach(s => {nameSet += s.getText})
  WordSegmenter.seg(briefName, SegmentationAlgorithm.MinimumMatching).forEach(s => {briefSet += s.getText})

  def getPossibility(str : List[String], d: Dictionary) : (orgClass, Double) = {
    val len = str.map(s => s.length).sum
    val pos = str.map(s => (s, s.length, nameSet.contains(s), briefSet.contains(s)))
      .map(p => {
        if (!(p._4 || p._3)) p._2 * (-0.5) // 错值惩罚
        else {
          p._2 * d.possibility(p._1) // 稀少奖励 // 正值奖励
        }
      }).map(f => (f + 0.0) / (len * 1.0)).sum
    (this, pos)
  }

  def getList : List[String] = List(code, id, name, briefName, city, province)
}

class Dictionary(val orgList : List[orgClass]) {
  val cat: List[String] = orgList.map(_.nameSet.toList).reduce(_ ::: _)
  val len: Int = orgList.size
  val possible: Map[String, Double] = cat.groupBy(a => a).map(a => (a._1, a._2.length)).map(a => (a._1, Dictionary.tur((a._2 + 1.0) / len)))
  def possibility(string: String) : Double = if (!possible.contains(string)) 0 else Math.log(possible(string))
}

object Dictionary{
  def tur(double: Double) : Double = {
    (1 / double) - 1
  }

}


class matchClass(val appearName : String) {
//  论文里的名字,组织机构ID,组织机构代码,组织机构名称,组织机构简称,所属地市,所属省份
  var li : List[String] = List[String]()
  WordSegmenter.seg(appearName, SegmentationAlgorithm.MinimumMatching).forEach(f => li :+= f.getText)
}
object matchClass {
  def apply(file : File) : List[matchClass] = {
    var ret = List[matchClass]()
    val bufferedSource = io.Source.fromFile(file,"UTF-8")
    for (line <- bufferedSource.getLines) {
      val cols = line.split(",").map(_.trim)
      ret :+= new matchClass(cols(0))
    }
    bufferedSource.close
    ret
  }
}
object orgClass{
  def apply(file : File) : List[orgClass] = {
    var ret = List[orgClass]()
    val bufferedSource = io.Source.fromFile(file,"UTF-8")
    for (line <- bufferedSource.getLines) {
      val cols = line.split(",").map(_.trim)
      ret :+= new orgClass(cols(0), cols(1), cols(2), cols(3), cols(4), cols(5))
    }
    bufferedSource.close
    ret
  }

  def writeFile(ls : Array[String], append : Boolean, outputPath : String, outputName : String) : Unit = {
    val f: File = new File(outputPath)
    if (!f.exists()) f.mkdirs()
    val out = new FileWriter(outputPath + outputName,append)
    for (i <- ls) out.write(i + "\r\n")
    out.close()
  }

  def main(args: Array[String]): Unit = {
    val matchList : List[matchClass] = matchClass(new File("C:\\Users\\coco1\\IdeaProjects\\textMatch\\src\\main\\组织机构_国家局_论文_合并.csv"))
    val dicList : List[orgClass] = orgClass(new File("C:\\Users\\coco1\\IdeaProjects\\textMatch\\src\\main\\组织机构_国家局.csv"))
    println("done init ")
    val dictionary = new Dictionary(dicList)
    val ls = matchList.map(f => (f, f.appearName)).map(s => {
      (s._1,
      dicList.map(dic => dic.getPossibility(s._1.li, dictionary)).map(s => {(s._1,s._2)}).maxBy(_._2)._1)
    }).map(s => (List(s._1.appearName) ::: s._2.getList).reduce(_ + "," + _)).toArray

    writeFile(ls, append = false, "C:\\Users\\coco1\\IdeaProjects\\textMatch\\src\\main\\", "outname.csv")



//    val lo = new matchClass("上海烟草(集团)上海卷烟厂").li
//    val tm = System.currentTimeMillis()
//    val str = dicList.map(dic => dic.getPossibility(lo, dictionary))
//    str.foreach(s => println(s._1.name + ":" + s._2))
////    lo.foreach(println(_))
//    println(s"${str.maxBy(_._2)._1.name + " , " + str.maxBy(_._2)._2} : ${System.currentTimeMillis() - tm}" )
//    str.map(_._1).foreach(a => println(a.nameSet))
  }
}
