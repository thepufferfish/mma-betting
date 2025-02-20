# MMA Betting Project

The aim of this project is to explore the feasibility of predicting results of MMA fights and creating betting strategies. The data pipeline is orchestrated by Dagster, with data coming from the UFC and various other sources. 

## Getting started

First, clone this repo, then navigate to the project folder and run the following command to install dependencies:

```bash
pip install -e ".[dev]"
```

Then, start the Dagster UI web server:

```bash
dagster dev
```

Open http://localhost:3000 with your browser to see the project.

## TODO:

* Improve linkage between data sources
* Ingest data from FightOddsAPI
* Scrape data from Sherdog or Tapology to get a bigger picture
  * Possibly use Graph Neural Networks (GNNs) to create embeddings to incorporate fights from outside the UFC which could be especially helpful for fighters with few or no UFC fights.
* Explore using Recurrent Neural Networks (RNNs) to create embeddings using pre-fight betting odds movement
* Test using Convolutional Neural Networks (CNNs) on strike data and grappling data to create embeddings of fighter offense and defense profiles
* Incorporate modeling frameworks such as MLFlow for monitoring
