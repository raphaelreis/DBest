
<!-- PROJECT LOGO -->
<br />
<p align="center">
  <h3 align="center">Scala DBEst</h3>

  <p align="center">
    <br />
    A <a href="https://www.scala-lang.org/"><strong>Scala</strong></a> implementation of DBEst Approximate Query Processing
    <br /> 
  </p>
</p>



<!-- TABLE OF CONTENTS -->
<details open="open">
  <summary>Table of Contents</summary>
  <ol>
    <li>
      <a href="#about-the-project">About The Project</a>
      <ul>
        <li><a href="#built-with">Built With</a></li>
      </ul>
    </li>
    <li>
      <a href="#getting-started">Getting Started</a>
      <ul>
        <li><a href="#prerequisites">Prerequisites</a></li>
        <li><a href="#installation">Installation</a></li>
      </ul>
    </li>
    <li><a href="#usage">Usage</a></li>
    <!-- <li><a href="#roadmap">Roadmap</a></li> -->
    <!-- <li><a href="#contributing">Contributing</a></li> -->
    <li><a href="#license">License</a></li>
    <li><a href="#contact">Contact</a></li>
    <li><a href="#acknowledgements">Acknowledgements</a></li>
  </ol>
</details>



<!-- ABOUT THE PROJECT -->
## About The Project


The present design was realized for a Semester Project at [EPFL](https://www.epfl.ch/fr/) in collaboration with the [Laboratory for Data Intensive Applications & Systems](https://www.epfl.ch/labs/dias/) of Prof. [Anastasia Ailamaki](https://people.epfl.ch/anastasia.ailamaki) and under the supervision of the PhD student [Viktor Sanca](https://people.epfl.ch/viktor.sanca/?lang=en).

This project aimed at building a Scala implementation of the [DBEst](https://github.com/qingzma/DBEstClient) Approximation Query Processor (AQP) with the well known [Spark library](https://spark.apache.org/). Traditional AQPs rely on data sampling to approximate a query  anwer. DBEst is a novative AQP that approximate the answer based on Machine Learning models. This brings many advantages regarding the query response time, database portability and data transfer. Indeed, under certain error tolerance (which is manageable), the data is not required anymore to information from a certain database.

We decided to write a Spark based implementation of DBEst as a first step to extend the original DBEst implementation. One could analyse the perspectives of model-based querying in situation where there is constraints regarding the query responsiveness, the network data flow or the data storage.

Please find [here]() the report related to my work.

### Built With


As mention above, the implementation rely on Apache Spark Library (2.4.6) and Scala Lang (2.11.12). For the other libraries, please check the [buidl.sbt](https://github.com/raphaelreis/DBest/blob/master/build.sbt) file for further details.

Sbt(1.0.0) is also required to build the project.


<!-- GETTING STARTED -->
## Getting Started

Here are the steps to start building the project and run the analysis experiments.

* Please first download the code or import it through `git clone https://github.com/raphaelreis/DBest.git` command.

* Then you have to write on the `conf/configuration.conf` file. Mostly you have to setup your base directory path (the path to the directory of the project).

* You can setup the directory paths for the results running the script `scripts/setup.sh` from your working directory.

* Build the project in the working directory with the command `sbt package`

* To run all the experiments run the command `scripts/runexp_sample.sh 1 2 3`.

* There is also a script to run the model training beforehand `scripts/train_models.sh`.

<!-- LICENSE -->
## License

Distributed under the MIT License. See `LICENSE` for more information.



<!-- CONTACT -->
## Contact

Raphaël Reis Nunes - [@LinkedIn](https://www.linkedin.com/in/raphaelreisnunes/) - email: raphael.reisnunes at epfl dot ch




<!-- ACKNOWLEDGEMENTS -->
## Acknowledgements
* [DBEst original implementation](https://github.com/qingzma/DBEstClient)
