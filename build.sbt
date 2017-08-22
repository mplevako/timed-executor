name := "timed-executor"
version := "1.0"
scalaVersion := "2.12.3"
javacOptions in Compile ++= Seq("-source", "1.8", "-target", "1.8")

libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest"   % "3.0.3" % Test,
  "org.mockito"    % "mockito-all" % "1.10.19" % Test
)