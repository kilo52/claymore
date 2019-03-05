# Claymore: Additional Core Libraries for Java

[![Release](https://img.shields.io/badge/release-2.0.0-blue.svg)](https://github.com/kilo52/claymore/blob/master/release/2.0.0/) [![Download](https://img.shields.io/badge/download-jar-blue.svg)](https://github.com/kilo52/claymore/raw/master/release/2.0.0/claymore-2.0.0.jar)

Claymore is a set of core libraries which are often used in my Java projects. I wanted to make some of them open source in the hope that others might find them useful. The provided APIs are complementary to the native Java libraries, for example by adding support for DataFrames, readers/writers for CSV-files, a parser for command line arguments, and more.

## Getting Started

Unfortunately, Claymore is not available on Maven Central. You have to add it manually. Please see the documentation of your IDE on how to add external JARs to your build path.
However, if you are working with Maven you can simply add a local directory to your project where you can put all of your external third party JARs and instruct Maven to use it as a local repository. In order to do that, I usually just add a directory called *lib* to the root folder of my project and add the following to the *pom.xml*:
```
<repositories>
  <repository>
    <id>lib</id>
    <name>Local Project Repository</name>
    <url>file://${project.basedir}/lib/</url>
  </repository>
</repositories>
```
**NOTE:**
Be aware that Maven uses the default repository layout, so you have to rebuild the directory and subdirectory structure yourself. It should look like this:
`<your_project_root>/lib/com/kilo52/claymore/claymore/2.0.0/`

Then just place all downloaded JARs to the above location.
Now you can use Claymore like any other dependency:
```
<dependency>
  <groupId>com.kilo52.claymore</groupId>
  <artifactId>claymore</artifactId>
  <version>2.0.0</version>
</dependency>
```
It is highly recommended that you include the Javadocs and/or sources when using Claymore. Maven should find them automatically if you downloaded the *-javadoc.jar* and *-sources.jar* together with the binaries.

## Compatibility

Claymore requires **Java 8** or higher. 

## Documentation

Take a look at the [Developer Documentation](https://github.com/kilo52/claymore/wiki/Home).

## License

The Claymore library is licensed under the Apache License Version 2 - see the [LICENSE](LICENSE) for details.


