# Проектирование AI-систем: мини-проекты

Серия мини-проектов по проектированию AI-систем: от постановки задачи и архитектуры до данных и обоснования решения. Выполнены в рамках программы курса [«ИИ-архитектор» (OTUS)](https://otus.ru/lessons/ai-architect/).

## Кейсы

### TechnoMart: сервис персональных рекомендаций для ритейлера

Спроектирован AI-сервис рекомендаций и data-платформа под него: пайплайны, озеро данных, feature store, security layer и observability.

- [02_Model_C4](02_Model_C4/hw02_c4_arch.md) - архитектура сервиса по C4 Model (контейнеры, компоненты, sequence) и OpenAPI-контракт `POST /get_recommendation`
- [05_Data_Pipelines](05_Data_Pipelines/TechnoMart_Data_Pipelines.md) - data pipeline от источников до feature store, выбор хранилищ
- [06_Observability](06_Observability/TechnoMart_Security_Testing_Observability.md) - security layer (PII-санитизация, guardrails), RAG-метрики как release gates, observability с AI-метриками на Grafana-дашборде ([мокап](06_Observability/dashboard_mockup.png))
- [08_CICD_MLOps_Production](08_CICD_MLOps_Production/README.md) - поставка AI-сервиса: IaC (Terraform), CI/CD (GitHub Actions + Argo CD), MLOps-конвейер обучения (Airflow + MLflow), canary-релизы с авто-откатом

### Trip Assistant: умный помощник командировок

Ассистент, который сам планирует командировку: подбирает билеты и отели, сверяется с политикой компании через RAG, считает бюджет. Внутри мультиагентная система, рабочий прототип и решение по хостингу LLM.

- [03_Trip_assistant](03_Trip_assistant/README.md) - мультиагентная система (Supervisor + Workers), RAG-пайплайн, прототип на LangGraph
- [04_ADR](04_ADR/README.md) - анализ и решение по хостингу LLM: облачная модель по API или self-hosted, [pitch для CTO](04_ADR/pitch-cto.md)

### Sizing: прод-контур Llama-3-70B под 1000 RPM

Расчёт железа и стоимости инференса Llama-3-70B: VRAM (веса + KV-кэш), выбор GPU, сравнение Yandex Cloud и Cloud.ru, эффект батчинга и vLLM.

- [07_Price_Resource_Sizing](07_Price_Resource_Sizing/README.md) - sizing и стоимость прод-контура Llama-3-70B: FP16 140 ГБ / INT4 35 ГБ, рекомендация 5×A100-80 с vLLM + AWQ INT4, ~1,2–1,7 млн ₽/мес ([Excel-модель](07_Price_Resource_Sizing/Sizing_Llama3-70B.xlsx) с живыми формулами)

## Проекты

| Проект | О чём | Стек |
|---|---|---|
| [C4-архитектура AI-сервиса](02_Model_C4/hw02_c4_arch.md) | Контейнеры, компоненты и ключевой сценарий рекомендательного сервиса, контракт интеграции | C4 Model, Mermaid, OpenAPI |
| [Мультиагентный ассистент](03_Trip_assistant/README.md) | Дизайн агентов, RAG-пайплайн и рабочий прототип | LangGraph, YandexGPT / MOCK |
| [Хостинг LLM: SaaS или self-hosted](04_ADR/README.md) | Сравнение вариантов по стоимости, приватности, качеству и сложности поддержки; ADR и pitch для CTO | TCO-анализ, взвешенная матрица |
| [Data pipeline рекомендаций](05_Data_Pipelines/TechnoMart_Data_Pipelines.md) | Поток данных от источников до feature store, выбор хранилищ, защита от training-serving skew | Kafka, S3 + Iceberg, Flink, Spark, Feast, Redis, Qdrant |
| [Security, Testing & Observability](06_Observability/TechnoMart_Security_Testing_Observability.md) | Контур качества генеративного сценария: защита от утечек PII и prompt injection, RAG-метрики как гейты релиза, дашборд golden signals + AI-метрики (токены, стоимость запроса) | Presidio, NeMo Guardrails, Ragas, DeepEval, Langfuse, Prometheus + Grafana, Loki, Tempo, Vault |
| [Sizing прод-контура Llama-3-70B](07_Price_Resource_Sizing/README.md) | VRAM и GPU под 1000 RPM (веса + KV-кэш, roofline-модель), цены Yandex Cloud vs Cloud.ru, экономия от батчинга и vLLM, рекомендация прод-конфигурации | Roofline-модель, vLLM, AWQ INT4, Excel/Google Sheets |
| [IaC, CI/CD и MLOps-конвейеры](08_CICD_MLOps_Production/README.md) | Два независимых релизных цикла (код и модель), интеграция артефактов через release-PR (image + modelVersion), canary-релиз с авто-анализом и авто-откатом, 0–1 ручных шагов | Terraform, GitHub Actions, Argo CD/Rollouts, Airflow, MLflow |

## Темы

- Архитектура: C4 Model, sequence-диаграммы, OpenAPI-контракты
- GenAI-паттерны: RAG, мультиагентные системы (Supervisor + Workers)
- Данные: ELT в озеро (S3 + Iceberg), stream и batch-обработка, feature store
- Решения: ADR, взвешенные матрицы критериев, расчёт TCO
- Sizing: расчёт VRAM (веса + KV-кэш), roofline-модель пропускной способности, эффект батчинга и vLLM, сравнение стоимости GPU в облаках
- Безопасность: закрытый контур, 152-ФЗ, обезличивание PII, guardrails, защита от prompt injection
- Качество и наблюдаемость: RAG-метрики как release gates (Faithfulness, Answer Relevancy), golden signals и SLO, Grafana-дашборд с AI-метриками
- Поставка: IaC (Terraform), GitOps (Argo CD), CI/CD-гейты, связка кода и модели через Model Registry, canary-релизы и авто-откат

## Структура репозитория

```text
.
├── 02_Model_C4/          # TechnoMart: C4-архитектура + OpenAPI
│   └── README.md
├── 03_Trip_assistant/    # Trip Assistant: агенты + RAG + прототип
│   ├── README.md
│   ├── 01_architecture.md
│   ├── 02_rag_flow.md
│   └── trip_assistant.py
├── 04_ADR/               # Trip Assistant: ADR по хостингу LLM + pitch
│   ├── README.md
│   ├── adr/adr-000-llm-hosting.md
│   └── pitch-cto.md
├── 05_Data_Pipelines/    # TechnoMart: data pipeline и хранилища
│   └── README.md
├── 06_Observability/     # TechnoMart: security, testing, observability
│   ├── README.md
│   ├── dashboard_mockup.html
│   └── dashboard_mockup.png
├── 07_Price_Resource_Sizing/  # Sizing: VRAM, GPU и стоимость Llama-3-70B
│   ├── README.md
│   └── Sizing_Llama3-70B.xlsx
└── 08_CICD_MLOps_Production/  # TechnoMart: поставка AI-сервиса (IaC, CI/CD, MLOps)
    └── README.md
```

