# Trend Analysis

This project compares how YouTube Shorts and Long-Form videos trend across countries. It uses a Spark-based pipeline to process over 100,000 video records, extracting features like duration, geography, and content type to understand how each format behaves.

---

## Goals

- Compare trending duration and patterns of Shorts vs Long-Form
- Identify key features that influence visibility
- Analyze geographic spread and content themes

---

## Key Insights

- Shorts trend longer than expected and spread within regional clusters
- Long-Form content trends more globally, driven by engagement metrics
- Emotional hooks are common in Shorts; structured narratives in Long-Form

---

## Stack

- **Apache Spark**, **AWS S3**, **PostgreSQL**
- **Python**, **XGBoost**, **LightGBM**
- **Matplotlib**, **Scikit-learn**, **NLP**

---

## Files

- `run_pipeline.py` – Spark job: read → clean → enrich → save
- `setup_env.sh` – Environment setup (AWS, Spark, Java paths)
- `run.sh` – Script to run the full pipeline
- `results/` – Visuals and model outputs

---

## Run

```bash
bash run.sh
