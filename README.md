# spark-tutorial

# Local Environment

* Spark 1.6
* Hadoop 2.6
* Scala 1.10.6

# Cloudera Environment (Optional)

[Cloudera QuickStarts: CDH 5.7](http://www.cloudera.com/downloads/quickstart_vms/5-7.html)

## Cloudera Dependencies

```bash
#Common dependencies
sudo yum install -y wget

#Install Scala 2.10.6 (spark 1.6 Installed)
wget http://www.scala-lang.org/files/archive/scala-2.10.6.tgz
tar xvf scala-2.10.6.tgz
sudo mv scala-2.10.6 /usr/lib
sudo ln -s /usr/lib/scala-2.10.6 /usr/lib/scala

#Add scala dir to the User PATH
echo 'export PATH=$PATH:/usr/lib/scala/bin' >> ~/.bashrc


#Install SBT
wget https://dl.bintray.com/sbt/rpm/sbt-0.13.12.rpm
sudo yum install java-devel jpackage-utils
sudo rpm -i sbt-0.13.12.rpm
```
