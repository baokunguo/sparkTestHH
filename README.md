# spark Test
�������ã�
jdk1.8.0_20
scala-sdk-2.10.5
spark-assembly-1.4.1-hadoop2.0.0-cdh4.6.0

���뱾��JAR����
mvn install:install-file
    -Dfile=IKAnalyzer3.2.8.jar
    -DgroupId=org.wltea.ik-analyzer
    -DartifactId=ik-analyzer
    -Dversion=3.2.8
    -Dpackaging=jar
--- pom �ļ�
<dependency>
    <groupId>org.wltea.ik-analyzer</group>
    <artifactId>ik-analyzer</artifactId>
    <version>3.2.8</version>
</dependency>

--- ����pom�ļ�
<dependency>
    <groupId>org.wltea.ik-analyzer</group>
    <artifactId>ik-analyzer</artifactId>
    <version>3.2.8</version>
    <systemPath>C:\Users\xyy\Desktop\a\IKAnalyzer3.2.8.jar</systemPath>
</dependency>
