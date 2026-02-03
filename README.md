# Analytic-Dashboard

# 🚀 Real-Time Analytics Dashboard

[![FastAPI](https://img.shields.io/badge/FastAPI-009688?style=flat&logo=fastapi&logoColor=white)](https://fastapi.tiangolo.com/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-316192?style=flat&logo=postgresql&logoColor=white)](https://www.postgresql.org/)
[![Redis](https://img.shields.io/badge/Redis-DC382D?style=flat&logo=redis&logoColor=white)](https://redis.io/)
[![SQLAlchemy](https://img.shields.io/badge/SQLAlchemy-FCA121?style=flat&logo=python&logoColor=black)](https://www.sqlalchemy.org/)
[![Docker](https://img.shields.io/badge/Docker-2496ED?style=flat&logo=docker&logoColor=white)](https://www.docker.com/)

&gt; **Enterprise-grade analytics backend** demonstrating complex SQL (window functions, CTEs), real-time data processing, and query optimization strategies.

## 📋 Table of Contents
- [Analytic-Dashboard](#analytic-dashboard)
- [🚀 Real-Time Analytics Dashboard](#-real-time-analytics-dashboard)
  - [📋 Table of Contents](#-table-of-contents)
  - [🎯 Overview](#-overview)
  - [🏗️ Architecture](#️-architecture)
  - [┌─────────────┐       ┌─────────────┐       ┌─────────────┐](#--------------)
  - [✨ Features](#-features)
    - [🔍 Complex SQL Analytics](#-complex-sql-analytics)
    - [⚡ Performance Optimizations](#-performance-optimizations)
    - [🔄 Real-time Capabilities](#-real-time-capabilities)
  - [🛠️ Tech Stack](#️-tech-stack)
  - [🚀 Quick Start](#-quick-start)
    - [Prerequisites](#prerequisites)
    - [1. Clone \& Setup](#1-clone--setup)
- [Copy environment variables](#copy-environment-variables)

---

## 🎯 Overview

This project simulates an e-commerce analytics platform that handles **1000+ events/second**, providing:
- **Real-time dashboards** with sub-second query performance
- **Cohort analysis** (retention tracking over time)
- **Funnel analytics** (conversion drop-off analysis)
- **RFM segmentation** (customer lifetime value analysis)
- **Anomaly detection** (Z-score based traffic monitoring)

Built to showcase production-grade backend engineering skills including database optimization, async programming, and complex analytical SQL.

---

## 🏗️ Architecture
```bash

 ┌─────────────┐       ┌─────────────┐       ┌─────────────┐ 
-------------------------------------------------------------
 │    users    │───────│   events    │       │   orders    │ 
 ├─────────────┤       ├─────────────┤       ├─────────────┤ 
 │ id (PK)     │       │ id (PK)     │       │ id (PK)     │ 
 │ email       │◄──────│ user_id (FK)│       │ user_id (FK)│ 
 │ created_at  │       │ session_id  │       │ order_number│ 
 │ acquisition │       │ event_type  │       │ amount      │ 
 │ country     │       │ page_path   │       │ status      │ 
 │ device_type │       │ metadata    │       │ metadata    │ 
 └─────────────┘       │ created_at  │       │ created_at  │ 
 └─────────────┘       └─────────────┘                       

```


---

## ✨ Features

### 🔍 Complex SQL Analytics
- **Window Functions**: `ROW_NUMBER()`, `LAG()`, `LEAD()`, `NTILE()`, `FIRST_VALUE()`
- **CTEs**: Multi-level Common Table Expressions for funnel analysis
- **Time-series**: Bucketing, rolling averages, year-over-year comparisons
- **Cohort Analysis**: Retention matrices with time-based partitioning

### ⚡ Performance Optimizations
- **Materialized Views**: Pre-aggregated metrics (5-minute refresh)
- **Redis Caching**: Sub-10ms response for hot queries
- **Strategic Indexing**: BRIN for time-series, GIN for JSON, Partial indexes
- **Connection Pooling**: Async database connections (20+ concurrent)

### 🔄 Real-time Capabilities
- **WebSocket Support**: Live dashboard updates
- **Event Streaming**: 1000+ events/second ingestion
- **Background Jobs**: Automated materialized view refresh

---

## 🛠️ Tech Stack

| Layer | Technology |
|-------|-----------|
| **Backend** | Python 3.11+, FastAPI, Uvicorn |
| **Database** | PostgreSQL 15, asyncpg, SQLAlchemy 2.0 |
| **Cache** | Redis 7 (async redis-py) |
| **Queue** | Celery (for background tasks) |
| **DevOps** | Docker, Docker Compose |
| **Testing** | pytest, pytest-async |

---

## 🚀 Quick Start

### Prerequisites
- Python 3.11+
- Docker & Docker Compose
- Git

### 1. Clone & Setup
```bash
git clone https://github.com/yourusername/analytics-dashboard.git
cd analytics-dashboard

# Copy environment variables
cp backend/.env.example backend/.env
