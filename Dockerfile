FROM martin/spark-base-prodigen:latest 

RUN git clone https://github.com/martindacos/spark-conf.git /spark-2.1.0-bin-hadoop2.7/conf2
RUN cp /spark-2.1.0-bin-hadoop2.7/conf2/spark-defaults.conf /spark-2.1.0-bin-hadoop2.7/conf/spark-defaults.conf
RUN cp /spark-2.1.0-bin-hadoop2.7/conf2/spark-env.sh /spark-2.1.0-bin-hadoop2.7/conf/spark-env.sh
RUN git clone https://github.com/martindacos/prodigen-backend.git /opt/prodigen

RUN rm /opt/prodigen/out/artifacts/prodigen_backend_jar/logback-core-1.1.11.jar
RUN rm /opt/prodigen/out/artifacts/prodigen_backend_jar/logback-classic-1.1.11.jar

EXPOSE 8080 6066 7077 8083

CMD mongod --fork --logpath /var/log/mongodb/mongod.log && ./spark-2.1.0-bin-hadoop2.7/sbin/start-master.sh -h 172.17.0.2 && ./spark-2.1.0-bin-hadoop2.7/sbin/start-slave.sh spark://172.17.0.2:6066 && ./spark-2.1.0-bin-hadoop2.7/bin/spark-submit --class bpm.Application /opt/prodigen/out/artifacts/prodigen_backend_jar/prodigen-backend.jar