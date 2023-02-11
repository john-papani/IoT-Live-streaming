# SmartHome System

<p align="center"> 	This project was part of the Analysis and Design of Information Systems course at NTUA. (2022-23, Fall Semester) <p>

## IoT-Live-streaming
<p align="center">
<img src="https://c0.wallpaperflare.com/preview/216/276/927/business-city-communication-connection.jpg" width="700" height="400">
</p>

<br>

<p align="center">

| Name                                | Εmail                  | AM         |
| ----------------------------------- | ---------------------- | ---------- |
| Papanikolaou Ioannis                | *el18064@mail.ntua.gr* | 031 18 064 |
| Andreas Chrysovalantis-Konstantinos | *el18102@mail.ntua.gr* | 031 18 102 |
| Maniatis Andreas                    | *el18070@mail.ntua.gr* | 031 18 070 |

</p>


---

<br> 
 <details><summary> Οδηγίες εγκατάστασης εφαρμογής </summary>
<p>

1. Install Docker Desktop
2. Για να καταβάσεις όλα τα containers, τρέξε το αρχείο `./downloadAllDocker.sh` σε ένα terminal. Βεβαιώσου από το Docker Destrop έχουν κατέβει όλα τα απαραίτητα containers.
   Σε περίπτωση προβλήματος ακολούθησε τους παρακάτω συνδέσμους για εγκατάσταση: [RabbitMQ](https://www.rabbitmq.com/download.html), [Apache Flink](https://nightlies.apache.org/flink/flink-docs-release-1.16/docs/deploymentresource-providers/standalone/docker/), [OpenTSDB](https://hub.docker.com/r/petergrace/opentsdb-docker/), [Grafana](https://grafana.com/docs/grafana/latest/setup-grafana/installation/docker)

3. Εγκατέστησε την [Java 11](https://www.oracle.com/java/technologies/javase/jdk11-archive-downloads.html) - _προσοχή με τα path_.
4. Εγκατέστησε την [Python](https://www.python.org/downloads/release/python-3108/).

5. Εγκατέστησε την [Maven Apach - 3.8.6](https://maven.apache.org/install.html) - **Αν χρειαστεί άλλαξε στις οδηγίες σε _.3.8.6_**

---

5. Κατέβασε τοπικά το repository (clone || download-zip).
6. Έπειτα `cd ...`.
7. Τρέξε το αρχείο `./installMvnFlink.sh`

</p>
</details>
<br>

---

<br>
<details><summary> Οδηγίες λειτουργίας εφαρμογής </summary>
<p>

Σε δύο terminal τρέχουμε ταυτόχρονα το java αρχείο [`all_aggregation.java`](www..gogole.com) και το python αρχείο [`send.py`](image.png).<br>
Έπειτα, τα δεδομένουν έχουν επεξεργαστεί και αποθηκευτεί στην βάση δεδομένων. Άρα είμαστε σε θέση μέσω του Grafana να παρατηρήσουμε τα διαγραμμάτα και τους πίνακες.

</p>
</details>

<br>

---


<br>
<details><summary> Οδηγίες Εκκαθάρισης Βάσης Δεδομένων</summary>
<p>

Σε περίπτωση που επιθυμούμε να διαγράψουμε ολα τα δεδομένα από την βάση δεδομένων, ανοίγουμε το terminal εσωτερικά του docker, στο **opentsdb container** και τρέχουμε τις παρακάτω εντολές.
> **Προσοχή**: Οι παρακάτω εντολές οδηγούν στην οριστική διαγραφή των δεδομένων.

```sh
tsdb scan 2022/01/01 --delete  none th1
tsdb scan 2022/01/01 --delete  none th2
tsdb scan 2022/01/01 --delete  none hvac1
tsdb scan 2022/01/01 --delete  none hvac2
tsdb scan 2022/01/01 --delete  none miac1
tsdb scan 2022/01/01 --delete  none miac2
tsdb scan 2022/01/01 --delete  none etot
tsdb scan 2022/01/01 --delete  none mov1
tsdb scan 2022/01/01 --delete  none wtot
tsdb scan 2022/01/01 --delete  none w1

tsdb scan 2022/01/01 --delete  none avgTh1
tsdb scan 2022/01/01 --delete  none avgTh2
tsdb scan 2022/01/01 --delete  none sumHvac1
tsdb scan 2022/01/01 --delete  none sumHvac2
tsdb scan 2022/01/01 --delete  none sumMiac1
tsdb scan 2022/01/01 --delete  none sumMiac2
tsdb scan 2022/01/01 --delete  none maxEtot
tsdb scan 2022/01/01 --delete  none sumMov1
tsdb scan 2022/01/01 --delete  none sumW1

tsdb scan 2022/01/01 --delete  none diffMaxEtot
tsdb scan 2022/01/01 --delete  none diffMaxWtot
```
</p>
</details>

