ThisBuild / scalaVersion := "2.13.7"

addCompilerPlugin(
  "org.typelevel" % "kind-projector" % "0.13.2" cross CrossVersion.full
)

lazy val root = (project in file("."))
  .settings(
    libraryDependencies ++= Seq(
      "org.typelevel" %% "cats-effect" % "3.3.9"
    )
  )
