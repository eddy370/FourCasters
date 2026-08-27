# FourCasters

Projet de collecte et d'analyse de données environnementales (météo, crues, hydrologie, incendies) à partir de plusieurs APIs publiques.

## Structure du projet

```
fourcasters/
│
├── notebooks/
│   ├── 01_open_meteo_forecast.ipynb
│   ├── 02_open_meteo_flood.ipynb
│   ├── 03_hubeau.ipynb
│   └── 04_nasa_firms.ipynb
│
├── src/
│
├── data/
│   ├── raw/
│   └── processed/
│
├── docs/
│
├── requirements.txt
├── .gitignore
└── README.md
```

## Sources de données

- **Open-Meteo Forecast API** — prévisions météorologiques
- **Open-Meteo Flood API** — prévisions de crues
- **Hub'Eau** — données hydrologiques françaises
- **NASA FIRMS** — détection d'incendies actifs

## Installation

```bash
pip install -r requirements.txt
```
