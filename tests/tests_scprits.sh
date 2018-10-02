deps=target/CrunchifySpringMVCFramework-0.0.1-SNAPSHOT-jar-with-dependencies.jar
serv () { cd /Users/Kiarash/dev/eclipse_workspace/Driver/target/classes; java sync.Scheduler; }

#
init () { java -cp $deps App init $1; }
ur () { java -cp $deps App updateReservation $1;} 
ff () { java -cp $deps App findFlights $1; }
fos () { java -cp $deps App findOpenSeats $1; }

a1 () { init -1; wait; ur 1 & ff 2 & fos 3; wait; }
