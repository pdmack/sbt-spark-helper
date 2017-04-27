package io.radanalytics.sbt.plugin.sbtsparkhelper

import sbt._
import java.nio.charset.Charset

object SbtSparkHelper {
  private[sbtsparkhelper] val DEFAULT_PACKAGES_FILE_NAME = ".app-packages"
  private[sbtsparkhelper] val DEFAULT_JARS_FILE_NAME = ".app-jars"

  def doPackages(allModules: Seq[ModuleID], packagesFile: File, log: Logger): File = {
    val moduleLines = allModules
      .filter(m => m.organization != "org.scala-lang")
      .filter(m => m.organization != "org.apache.spark")
      .filter(m => m.organization != "org.apache.hadoop")
      .map(mod => s"""${mod.organization}:${mod.name}:${mod.revision}""")
      .sorted
      .mkString(",")

    IO.write(packagesFile, moduleLines)
    log.info(s"$packagesFile generated.")
    packagesFile
  }

  def doJars(allJars: Seq[File], jarsFile: File, log: Logger): File = {
    val jarLines = allJars
      .sorted
      .mkString(",")

    IO.write(jarsFile, jarLines)
    log.info(s"$jarsFile generated.")
    jarsFile
  }

  def packagesFile(state: State): File = {
    val extracted = Project.extract(state)
    val buildStruct = extracted.structure
    val buildUnit = buildStruct.units(buildStruct.root)

    val packagesFileName = EvaluateTask.getSetting(SbtSparkHelperKeys.sbtSparkPackagesFile, DEFAULT_PACKAGES_FILE_NAME, extracted, buildStruct)
    new File(buildUnit.localBase, packagesFileName)
  }

}
